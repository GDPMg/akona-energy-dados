"""
### Akona Energy · coleta diária (GS FIAP 2026.2 · CitizenTech)

Todo dia às **06:00** (horário de Brasília) este DAG coleta os dados do **dia anterior**
e grava **um arquivo por fonte** na pasta `chegada/`, com a data no nome:

| Fonte | Arquivo | Origem |
|---|---|---|
| Clima de Osasco | `chegada/clima/clima_AAAA-MM-DD.json` | API pública Open-Meteo (sem chave) |
| Município e Censo 2022 | `chegada/ibge/ibge_AAAA-MM-DD.json` | API pública do IBGE |
| Chamados dos moradores | `chegada/chamados/chamados_AAAA-MM-DD.json` | API do Akona (simulada, exige token) |

Fluxo: `inicio >> [coletar_clima, coletar_ibge]` (em paralelo) · `coletar_clima >> simular_chamados`
(o simulador usa o clima do dia) · `[simular_chamados, coletar_ibge] >> conferir_chegada >> notificar`
· `conferir_chegada >> lakehouse_automatico >> disparar_lakehouse` (envia o dia ao Databricks e roda as camadas).

Configuração e segredos ficam em **Airflow Variables**, nunca no código:
`akona_config` (JSON com coordenadas e código IBGE; tem valores padrão),
`akona_api_token` (obrigatório), `akona_webhook_secret` (opcional, webhook do Discord) e
`akona_lakehouse_automatico` (`true` liga o disparo diário do DAG `akona_lakehouse`).
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Variable

_PASTA_DAGS = str(Path(__file__).resolve().parent)  # permite importar o pacote dags/akona
if _PASTA_DAGS not in sys.path:  # no fim da lista: a pasta montada do Windows é lenta para buscas
    sys.path.append(_PASTA_DAGS)
from akona.notificacao import avisar_falha, enviar_notificacao  # noqa: E402
from akona.simulador_chamados import gerar_arquivo_chamados  # noqa: E402

TZ = "America/Sao_Paulo"
CHEGADA = Path(os.environ.get("AKONA_CHEGADA", "/opt/airflow/chegada"))
FONTES = ("clima", "ibge", "chamados")
IBGE_API = "https://servicodados.ibge.gov.br/api"
CONFIG_PADRAO = {
    "municipio": "Osasco",
    "cod_ibge": "3534401",
    "latitude": -23.5325,
    "longitude": -46.7917,
    "timezone": TZ,
    "openmeteo_url": "https://api.open-meteo.com/v1/forecast",
    "inicio_simulacao": "2026-10-01",
}

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------------- apoio

def _config() -> dict:
    """Configuração vinda da Variable akona_config (sobrepõe os valores padrão)."""
    return {**CONFIG_PADRAO, **Variable.get("akona_config", default={}, deserialize_json=True)}


def _dia_coletado(context) -> str:
    """A execução das 06:00 do dia D coleta o dia D-1 inteiro."""
    referencia = context.get("logical_date") or pendulum.now(TZ)
    return pendulum.instance(referencia).in_timezone(TZ).subtract(days=1).to_date_string()


def _gravar(fonte: str, dia: str, conteudo: str) -> Path:
    """Grava chegada/<fonte>/<fonte>_<dia>.json de forma atômica (.tmp + rename)."""
    destino = CHEGADA / fonte / f"{fonte}_{dia}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(conteudo, encoding="utf-8")
    temporario.replace(destino)
    return destino


# ----------------------------------------------------------------------------- tarefas

def coletar_clima(**context) -> str:
    """Clima horário de Osasco no dia coletado: chuva, vento, rajada, temperatura e código do tempo."""
    cfg, dia = _config(), _dia_coletado(context)
    resposta = requests.get(
        cfg["openmeteo_url"],
        params={
            "latitude": cfg["latitude"],
            "longitude": cfg["longitude"],
            "start_date": dia,
            "end_date": dia,
            "timezone": cfg["timezone"],
            "hourly": "precipitation,wind_speed_10m,wind_gusts_10m,temperature_2m,weather_code",
        },
        timeout=30,
    )
    resposta.raise_for_status()
    destino = _gravar("clima", dia, resposta.text)  # resposta bruta da API
    log.info("Clima de %s salvo em %s (%d bytes)", dia, destino, destino.stat().st_size)
    return destino.name


def coletar_ibge(**context) -> str:
    """Dados do município no IBGE: localidade + Censo 2022 (população, área, densidade, domicílios)."""
    cfg, dia = _config(), _dia_coletado(context)
    codigo = cfg["cod_ibge"]
    localidade = {"localidades": f"N6[{codigo}]"}  # N6 = nível município

    def baixar(caminho: str, params: dict | None = None):
        resposta = requests.get(f"{IBGE_API}/{caminho}", params=params, timeout=30)
        resposta.raise_for_status()
        return resposta.json()

    pacote = {  # envelope com as 3 respostas brutas da API
        "fonte": "IBGE - API de localidades e de agregados (Censo 2022)",
        "dia_coleta": dia,
        "coletado_em": pendulum.now(TZ).to_iso8601_string(),
        "municipio": baixar(f"v1/localidades/municipios/{codigo}"),
        # tabela 4714: população residente (93), área (6318) e densidade (614)
        "censo_populacao": baixar("v3/agregados/4714/periodos/2022/variaveis/93|6318|614", localidade),
        # tabela 4712: domicílios ocupados (381), moradores (382) e média de moradores por domicílio (5930)
        "censo_domicilios": baixar("v3/agregados/4712/periodos/2022/variaveis/381|382|5930", localidade),
    }
    destino = _gravar("ibge", dia, json.dumps(pacote, ensure_ascii=False))
    log.info("IBGE de %s salvo em %s", dia, destino)
    return destino.name


def simular_chamados(**context) -> dict:
    """Chamados abertos e atualizados no dia, vindos da API do Akona (simulada)."""
    cfg, dia = _config(), _dia_coletado(context)
    token = Variable.get("akona_api_token", default=None)
    if not token:
        raise RuntimeError("Crie a Variable 'akona_api_token' (Admin > Variables) antes de rodar o DAG.")
    destino, resumo = gerar_arquivo_chamados(dia, CHEGADA, token=token,
                                             inicio_simulacao=cfg["inicio_simulacao"])
    log.info("Chamados de %s salvos em %s: %s", dia, destino, resumo)
    return resumo


def conferir_chegada(**context) -> dict:
    """Confere se os 3 arquivos do dia chegaram, não estão vazios e são JSON válido."""
    dia = _dia_coletado(context)
    resumo, problemas = {}, []
    for fonte in FONTES:
        arquivo = CHEGADA / fonte / f"{fonte}_{dia}.json"
        if not arquivo.exists() or arquivo.stat().st_size == 0:
            problemas.append(f"{arquivo.name}: não chegou ou está vazio")
            continue
        linhas = [linha for linha in arquivo.read_text(encoding="utf-8").splitlines() if linha.strip()]
        try:
            for linha in linhas:
                json.loads(linha)
        except json.JSONDecodeError as erro:
            problemas.append(f"{arquivo.name}: JSON inválido ({erro})")
            continue
        resumo[fonte] = {"arquivo": arquivo.name, "bytes": arquivo.stat().st_size, "registros": len(linhas)}
    if problemas:
        raise FileNotFoundError("Conferência da chegada falhou: " + "; ".join(problemas))
    log.info("Chegada conferida para %s: %s", dia, resumo)
    return resumo


def notificar(**context) -> None:
    """Notificação de sucesso com o resumo da coleta."""
    dia = _dia_coletado(context)
    resumo = context["ti"].xcom_pull(task_ids="conferir_chegada") or {}
    partes = ", ".join(f"{fonte}: {info['registros']} registro(s)" for fonte, info in resumo.items())
    enviar_notificacao(f"✅ Akona Energy · coleta de {dia} concluída ({partes}).")


def lakehouse_automatico() -> bool:
    """Só dispara o lakehouse se a Variable akona_lakehouse_automatico for 'true' (senão, pula)."""
    return str(Variable.get("akona_lakehouse_automatico", default="false")).strip().lower() == "true"


# ----------------------------------------------------------------------------- DAG

with DAG(
    dag_id="akona_coleta_diaria",
    description="Akona Energy: coleta diária de chamados, clima e IBGE de Osasco para a pasta chegada/",
    doc_md=__doc__,
    schedule="0 6 * * *",                               # todo dia às 06:00
    start_date=pendulum.datetime(2026, 10, 2, 6, tz=TZ),  # 1ª execução coleta 01/10/2026
    catchup=True,                                       # executa os dias passados (backfill)
    max_active_runs=1,                                  # um dia de cada vez, em ordem
    default_args={
        "owner": "akona-dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": avisar_falha,
    },
    tags=["akona", "gs-2026", "coleta"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    ibge = PythonOperator(task_id="coletar_ibge", python_callable=coletar_ibge)
    chamados = PythonOperator(task_id="simular_chamados", python_callable=simular_chamados)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada, retries=0)
    aviso = PythonOperator(task_id="notificar", python_callable=notificar)
    automatico = ShortCircuitOperator(task_id="lakehouse_automatico", python_callable=lakehouse_automatico)
    disparar = TriggerDagRunOperator(
        task_id="disparar_lakehouse",
        trigger_dag_id="akona_lakehouse",
        conf={  # o dia coletado é o dia anterior ao da execução
            "de": "{{ macros.ds_add(ds, -1) }}",
            "ate": "{{ macros.ds_add(ds, -1) }}",
            "lote": "diario_{{ macros.ds_add(ds, -1) }}",
        },
        wait_for_completion=False,
    )

    inicio >> [clima, ibge]
    clima >> chamados
    [chamados, ibge] >> conferir >> aviso
    conferir >> automatico >> disparar
