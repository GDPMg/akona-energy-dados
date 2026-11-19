"""
Tarefas reutilizáveis pelas DAGs. Fontes, URLs e tabelas vêm de config/.

Parâmetro `modo` das DAGs de fonte: incremental (dia anterior), data_especifica (de → ate) ou full (inicio_dados → ontem).
Em qualquer modo o que já existe é pulado, então o full só preenche os dias que faltarem.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import Param, Variable

from akona import apis, chegada, configuracao
from akona.databricks_api import Databricks

PASTA_NOTEBOOKS = Path(os.environ.get("AKONA_NOTEBOOKS", "/opt/airflow/databricks"))
log = logging.getLogger(__name__)

# 1 nova tentativa: as camadas são idempotentes
OPCOES_NOTEBOOK = {"retries": 1, "retry_delay": timedelta(seconds=30), "execution_timeout": timedelta(minutes=60)}

MODOS = {
    "incremental": "Incremental: só o dia anterior (padrão, roda todo dia)",
    "data_especifica": "Data específica: o período de → até",
    "full": "Full: todos os dias desde o início dos dados (01/10/2026) até ontem",
}

PARAMETROS_FONTE = {
    "modo": Param("incremental", type="string", enum=list(MODOS), values_display=MODOS, title="Modo",
                  description="incremental = dia anterior; data_especifica = de → até; "
                              "full = desde o início dos dados até ontem (só preenche o que faltar)."),
    # None permite deixar vazio no formulário
    "de": Param(None, type=["null", "string"], title="De (AAAA-MM-DD)",
                description="Só no modo data_especifica: primeiro dia."),
    "ate": Param(None, type=["null", "string"], title="Até (AAAA-MM-DD)",
                 description="Só no modo data_especifica: último dia. Vazio = só o dia 'de'."),
    "lote": Param(None, type=["null", "string"], title="Rótulo do lote (opcional)",
                  description="Gravado no log da Bronze (ex.: lote_1). Vazio = diario_<dia>, especifico_<de>_a_<ate> ou full_<ate>."),
}


def periodo(params) -> dict:
    """Resolve o período da execução a partir do modo (incremental, data_especifica ou full)."""
    ontem = (datetime.now(ZoneInfo(configuracao.projeto()["timezone"])) - timedelta(days=1)).date().isoformat()
    modo = str(params.get("modo") or "incremental").strip()
    if modo == "incremental":
        de = ate = ontem
        lote_padrao = f"diario_{ontem}"
    elif modo == "data_especifica":
        de = str(params.get("de") or "").strip()
        if not de:
            raise ValueError("No modo data_especifica, informe o parâmetro 'de' (e 'ate', se for mais de um dia).")
        ate = str(params.get("ate") or "").strip() or de
        lote_padrao = f"especifico_{de}" if de == ate else f"especifico_{de}_a_{ate}"
    elif modo == "full":
        de, ate = configuracao.projeto()["inicio_dados"], ontem
        lote_padrao = f"full_{ate}"
    else:
        raise ValueError(f"Modo '{modo}' inválido. Use um destes: {', '.join(MODOS)}.")
    chegada.dias_do_periodo(de, ate)                                    # valida as datas
    lote = str(params.get("lote") or "").strip() or lote_padrao
    return {"modo": modo, "de": de, "ate": ate, "lote": lote}


def _contexto_da_fonte(fonte: dict) -> dict:
    """Valores disponíveis como {campo} no YAML: config/projeto.yaml + segredos (Airflow Variables)."""
    contexto = {k: v for k, v in configuracao.projeto().items() if not isinstance(v, dict)}
    contexto["pasta_chegada"] = str(chegada.PASTA_CHEGADA)
    for campo, variavel in fonte["segredos"].items():
        valor = Variable.get(variavel, default=None)
        if not valor:
            raise RuntimeError(f"Crie a Variable '{variavel}' (Admin > Variables): ela é usada pela fonte {fonte['nome']}.")
        contexto[campo] = valor
    return contexto


def coletar(fonte: str, **context) -> dict:
    """Chama a API da fonte para cada dia do período e grava chegada/<fonte>/<fonte>_<dia>.json.
    Dia que já tem arquivo não chama a API de novo: a coleta só preenche os dias que faltam."""
    cfg, p = configuracao.fonte(fonte), periodo(context["params"])
    contexto = _contexto_da_fonte(cfg)
    dias = chegada.dias_do_periodo(p["de"], p["ate"])
    coletados = []
    for dia in dias:
        destino = chegada.arquivo_do_dia(fonte, dia)
        if destino.exists():
            continue
        chegada.gravar(destino, apis.extrair(cfg, dia, {**contexto, "dia": dia}))
        coletados.append(dia)
    resumo = {"fonte": fonte, "modo": p["modo"], "de": p["de"], "ate": p["ate"], "dias_no_periodo": len(dias),
              "ja_existiam": len(dias) - len(coletados), "coletados": len(coletados), "dias_coletados": coletados}
    log.info("Coleta (%s, %s a %s): %s", p["modo"], p["de"], p["ate"],
             f"{len(coletados)} dia(s) faltando foram coletados: {coletados}" if coletados
             else f"nenhum dia faltando ({len(dias)} dia(s) já coletados)")
    return resumo


def conferir_chegada(fonte: str, **context) -> dict:
    """Confere se o arquivo de cada dia do período chegou, não está vazio e é JSON válido."""
    p = periodo(context["params"])
    registros, problemas = 0, []
    dias = chegada.dias_do_periodo(p["de"], p["ate"])
    for dia in dias:
        try:
            registros += chegada.conferir(chegada.arquivo_do_dia(fonte, dia))
        except (FileNotFoundError, ValueError) as erro:
            problemas.append(str(erro))
    if problemas:
        raise FileNotFoundError(f"Conferência da chegada falhou ({len(problemas)} problema(s)): " + "; ".join(problemas[:10]))
    resumo = {"fonte": fonte, "arquivos": len(dias), "registros": registros}
    log.info("Chegada conferida: %s", resumo)
    return resumo


def _databricks() -> Databricks:
    host = Variable.get("akona_databricks_host", default=None)
    token = Variable.get("akona_databricks_token", default=None)
    if not host or not token:
        raise RuntimeError("Crie as Variables 'akona_databricks_host' e 'akona_databricks_token' (Admin > Variables).")
    return Databricks(host, token)


def _base_volume() -> str:
    d = configuracao.databricks()
    return f"/Volumes/{d['catalogo']}/{d['schema']}/{d['volume']}"


def preparar_databricks(pastas: list | tuple = (), segredo: bool = False) -> dict:
    """Cria, se não existirem: schema, Volume, pastas no Volume e (opcional) o segredo do salt LGPD."""
    d, db = configuracao.databricks(), _databricks()
    resumo = {
        "schema": db.garantir_schema(d["catalogo"], d["schema"], "Akona Energy: pipeline de dados da GS FIAP 2026.2"),
        "volume": db.garantir_volume(d["catalogo"], d["schema"], d["volume"], "Pasta de pouso: arquivos enviados pelo Airflow"),
    }
    for pasta in (*pastas, "_controle"):
        db.criar_pasta(f"{_base_volume()}/{pasta}")
    if segredo:
        resumo["salt_lgpd"] = db.garantir_segredo(d["escopo_segredos"], "salt_lgpd")
    log.info("Databricks pronto: %s", resumo)
    return resumo


def _arquivo_notebook(relativo: str) -> Path:
    for extensao in (".py", ".sql"):
        arquivo = PASTA_NOTEBOOKS / f"{relativo}{extensao}"
        if arquivo.exists():
            return arquivo
    raise FileNotFoundError(f"Notebook databricks/{relativo}(.py|.sql) não encontrado em {PASTA_NOTEBOOKS}")


def publicar_notebooks(notebooks: list[str]) -> list[str]:
    """Publica no workspace os notebooks indicados (caminhos relativos a databricks/, sem extensão)."""
    db, base = _databricks(), configuracao.databricks()["pasta_workspace"]
    publicados = []
    for relativo in notebooks:
        db.publicar_notebook(_arquivo_notebook(relativo), f"{base}/{relativo}")
        publicados.append(f"{base}/{relativo}")
    log.info("Notebooks publicados: %s", publicados)
    return publicados


def enviar_ao_volume(fonte: str, **context) -> dict:
    """Envia ao Volume os arquivos da fonte no período; os que já estão lá são pulados (idempotente)."""
    p, db = periodo(context["params"]), _databricks()
    enviados, ja_existiam = 0, 0
    for arquivo in chegada.arquivos_do_periodo(fonte, p["de"], p["ate"]):
        destino = f"{_base_volume()}/{fonte}/{arquivo.name}"
        if db.arquivo_existe(destino):
            ja_existiam += 1
            continue
        db.enviar_arquivo(arquivo, destino)
        enviados += 1
    resumo = {"fonte": fonte, "lote": p["lote"], "de": p["de"], "ate": p["ate"],
              "enviados": enviados, "ja_existiam": ja_existiam}
    log.info("Envio ao Volume: %s", resumo)
    return resumo


def rodar_notebook(notebook: str, nome: str, parametros: dict | None = None, enviar_lote: bool = False,
                   **context) -> dict:
    """Executa no Databricks (serverless) o notebook publicado e espera terminar. Falha se ele falhar."""
    parametros = dict(parametros or {})
    if enviar_lote:
        parametros["lote"] = periodo(context["params"])["lote"]
    caminho = f"{configuracao.databricks()['pasta_workspace']}/{notebook}"
    resultado = _databricks().rodar_notebook(caminho, parametros, nome=nome)
    log.info("%s concluído em %ss. Execução no Databricks: %s", nome, resultado["segundos"], resultado["url"])
    return resultado
