"""
### Akona Energy · lakehouse no Databricks (GS FIAP 2026.2 · CitizenTech)

Leva os arquivos da pasta `chegada/` para o Volume do Databricks e executa **no próprio Databricks**
(computação serverless) os notebooks de cada camada. O Airflow só orquestra: publica o código, envia os
arquivos, dispara cada notebook, espera o resultado e falha se o Databricks falhar.

Fluxo: `inicio >> [publicar_notebooks, preparar_databricks]` (em paralelo) · `preparar_databricks >> enviar_ao_volume`
· `[publicar_notebooks, enviar_ao_volume] >> bronze >> prata >> ouro >> consultas >> notificar`

| Tarefa | O que faz |
|---|---|
| publicar_notebooks | importa os notebooks da pasta `databricks/` do repositório para `/Shared/akona` (o que roda é sempre o que está no Git) |
| preparar_databricks | cria, se não existirem: schema `workspace.akona`, Volume `chegada`, pastas por fonte e o segredo do salt LGPD |
| enviar_ao_volume | envia os arquivos do período **de → até**; arquivos que já estão no Volume são pulados (envio idempotente) |
| bronze, prata, ouro, consultas | executam os notebooks 01 a 04 no Databricks e guardam o link de cada execução |

**Parâmetros** (Trigger com configuração): `de`, `ate` (AAAA-MM-DD) e `lote` (rótulo gravado no log de contagens da Bronze).
Lote 1 = outubro; lote 2 = novembro; reexecução = mesmo período de novo (0 arquivos novos, 0 linhas novas).
O DAG de coleta dispara este DAG todo dia quando a Variable `akona_lakehouse_automatico` = `true`.

Credenciais em **Airflow Variables**: `akona_databricks_host` e `akona_databricks_token` (mascarada).
"""
from __future__ import annotations

import logging
import os
import re
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, Variable

_PASTA_DAGS = str(Path(__file__).resolve().parent)  # permite importar o pacote dags/akona
if _PASTA_DAGS not in sys.path:  # no fim da lista: a pasta montada do Windows é lenta para buscas
    sys.path.append(_PASTA_DAGS)
from akona.databricks_api import Databricks  # noqa: E402
from akona.notificacao import avisar_falha, enviar_notificacao  # noqa: E402

TZ = "America/Sao_Paulo"
CHEGADA = Path(os.environ.get("AKONA_CHEGADA", "/opt/airflow/chegada"))
NOTEBOOKS_LOCAIS = Path(os.environ.get("AKONA_NOTEBOOKS", "/opt/airflow/databricks"))
CATALOGO, SCHEMA, VOLUME = "workspace", "akona", "chegada"
VOLUME_BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
PASTA_WORKSPACE = "/Shared/akona"
FONTES = ("chamados", "clima", "ibge")
CAMADAS = {  # tarefa do Airflow -> notebook no Databricks
    "bronze": "01_bronze_autoloader",
    "prata": "02_prata",
    "ouro": "03_ouro",
    "consultas": "04_consultas_mvp",
}
NOME_ARQUIVO = re.compile(r"^(chamados|clima|ibge)_(\d{4}-\d{2}-\d{2})\.json$")

log = logging.getLogger(__name__)


def _databricks() -> Databricks:
    host = Variable.get("akona_databricks_host", default=None)
    token = Variable.get("akona_databricks_token", default=None)
    if not host or not token:
        raise RuntimeError("Crie as Variables 'akona_databricks_host' e 'akona_databricks_token' (Admin > Variables).")
    return Databricks(host, token)


def publicar_notebooks() -> list:
    """Importa databricks/*.py e *.sql do repositório para o workspace (sobrescreve a versão anterior)."""
    db = _databricks()
    arquivos = sorted(NOTEBOOKS_LOCAIS.glob("*.py")) + sorted(NOTEBOOKS_LOCAIS.glob("*.sql"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum notebook em {NOTEBOOKS_LOCAIS}. A pasta databricks/ está montada no container?")
    publicados = []
    for arquivo in arquivos:
        destino = f"{PASTA_WORKSPACE}/{arquivo.stem}"
        db.publicar_notebook(arquivo, destino)
        publicados.append(destino)
    log.info("Notebooks publicados: %s", publicados)
    return publicados


def preparar_databricks() -> dict:
    """Cria a estrutura no Unity Catalog e o segredo do salt LGPD (tudo idempotente)."""
    db = _databricks()
    resumo = {
        "schema": db.garantir_schema(CATALOGO, SCHEMA, "Akona Energy: pipeline de dados da GS FIAP 2026.2"),
        "volume": db.garantir_volume(CATALOGO, SCHEMA, VOLUME, "Pasta de pouso: lotes enviados pelo Airflow"),
        "salt_lgpd": db.garantir_segredo("akona", "salt_lgpd"),
    }
    for pasta in (*FONTES, "_controle"):
        db.criar_pasta(f"{VOLUME_BASE}/{pasta}")
    log.info("Databricks pronto: %s", resumo)
    return resumo


def enviar_ao_volume(**context) -> dict:
    """Envia ao Volume os arquivos de chegada/ com data entre 'de' e 'ate'; pula os que já existem."""
    params = context["params"]
    de, ate, lote = str(params["de"]), str(params["ate"]), str(params["lote"])
    if de > ate:
        raise ValueError(f"Período inválido: de={de} é depois de ate={ate}")
    db = _databricks()
    enviados, ja_existiam = {fonte: 0 for fonte in FONTES}, 0
    for fonte in FONTES:
        for arquivo in sorted((CHEGADA / fonte).glob(f"{fonte}_*.json")):
            achado = NOME_ARQUIVO.match(arquivo.name)
            if not achado or not (de <= achado.group(2) <= ate):
                continue
            destino = f"{VOLUME_BASE}/{fonte}/{arquivo.name}"
            if db.arquivo_existe(destino):
                ja_existiam += 1
                continue
            db.enviar_arquivo(arquivo, destino)
            enviados[fonte] += 1
    resumo = {"lote": lote, "de": de, "ate": ate, "enviados": enviados,
              "total_enviados": sum(enviados.values()), "ja_existiam": ja_existiam}
    log.info("Envio ao Volume: %s", resumo)
    return resumo


def rodar_camada(camada: str, **context) -> dict:
    """Executa no Databricks o notebook da camada e espera terminar."""
    parametros = {"lote": context["params"]["lote"]} if camada == "bronze" else {}
    resultado = _databricks().rodar_notebook(f"{PASTA_WORKSPACE}/{CAMADAS[camada]}", parametros, nome=camada)
    log.info("Camada %s concluída em %ss. Execução no Databricks: %s", camada, resultado["segundos"], resultado["url"])
    return resultado


def notificar(**context) -> None:
    ti = context["ti"]
    envio = ti.xcom_pull(task_ids="enviar_ao_volume") or {}
    tempos = ", ".join(
        f"{c} {(ti.xcom_pull(task_ids=c) or {}).get('segundos', '?')}s" for c in CAMADAS)
    enviar_notificacao(
        f"✅ Akona Energy · lakehouse atualizado ({envio.get('lote')}: {envio.get('de')} a {envio.get('ate')}). "
        f"Arquivos novos no Volume: {envio.get('total_enviados')} (já existiam: {envio.get('ja_existiam')}). "
        f"Camadas: {tempos}.")


with DAG(
    dag_id="akona_lakehouse",
    description="Akona Energy: envia os arquivos ao Volume e executa Bronze, Prata, Ouro e consultas no Databricks",
    doc_md=__doc__,
    schedule=None,                                     # disparado pela coleta ou manualmente (com parâmetros)
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,                                 # uma carga de cada vez
    params={
        "de": Param("2026-10-01", type="string", format="date", title="De (AAAA-MM-DD)",
                    description="Primeiro dia de coleta a enviar"),
        "ate": Param("2026-10-31", type="string", format="date", title="Até (AAAA-MM-DD)",
                     description="Último dia de coleta a enviar"),
        "lote": Param("lote_1", type="string", title="Rótulo do lote",
                      description="lote_1, lote_2, reexecucao ou diario_AAAA-MM-DD"),
    },
    default_args={
        "owner": "akona-dados",
        "retries": 0,
        "on_failure_callback": avisar_falha,
    },
    tags=["akona", "gs-2026", "databricks"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    publicar = PythonOperator(task_id="publicar_notebooks", python_callable=publicar_notebooks, retries=1)
    preparar = PythonOperator(task_id="preparar_databricks", python_callable=preparar_databricks, retries=1)
    enviar = PythonOperator(task_id="enviar_ao_volume", python_callable=enviar_ao_volume, retries=2,
                            retry_delay=timedelta(minutes=1))
    camadas = [
        PythonOperator(task_id=camada, python_callable=rodar_camada, op_kwargs={"camada": camada},
                       execution_timeout=timedelta(minutes=60))
        for camada in CAMADAS
    ]
    aviso = PythonOperator(task_id="notificar", python_callable=notificar)

    inicio >> [publicar, preparar]
    preparar >> enviar
    [publicar, enviar] >> camadas[0]
    for anterior, seguinte in zip(camadas, camadas[1:]):
        anterior >> seguinte
    camadas[-1] >> aviso
