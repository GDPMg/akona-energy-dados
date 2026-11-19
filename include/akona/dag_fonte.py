"""
Padrão de DAG por fonte: API -> chegada/ -> Volume -> Bronze -> Prata.

Cada arquivo dags/akona_<fonte>.py só chama criar_dag_fonte("<fonte>"). Tudo o que muda de uma fonte
para outra está em config/fontes/<fonte>.yaml.

    inicio >> [coletar_api, preparar_databricks, publicar_notebooks]      (em paralelo)
    coletar_api >> conferir_chegada
    [conferir_chegada, preparar_databricks] >> enviar_ao_volume
    [enviar_ao_volume, publicar_notebooks] >> bronze >> prata
"""
from __future__ import annotations

import copy
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from akona import configuracao, tarefas
from akona.notificacao import avisar_falha

NOTEBOOK_COMUM = "comum/funcoes"
NOTEBOOK_BRONZE = "bronze/bronze_autoloader"


def _documentacao(fonte: dict) -> str:
    extracao = ("chamada genérica de API (`extrator: http`)" if fonte["extrator"] == "http"
                else f"função `{fonte['funcao']}` em `include/akona/apis.py`")
    dependencias = ", ".join(fonte["depende_de"]) or "nenhuma"
    return f"""
### Akona Energy · fonte **{fonte['nome']}**
{fonte['descricao']}

Processo completo da fonte, até a camada Prata: {extracao} → `chegada/{fonte['nome']}/` → Volume do Databricks
→ `bronze_{fonte['nome']}` (Auto Loader) → `{fonte['prata'].get('tabela', 'prata')}` (notebook `{fonte['prata']['notebook']}`).

- Configuração: `config/fontes/{fonte['nome']}.yaml` · depende das fontes: {dependencias}
- **Parâmetro `modo`** (Trigger com configuração):
  - `incremental` (padrão): só o dia anterior. É o que a `trigger-master` dispara todo dia.
  - `data_especifica`: o período `de` → `ate` (ex.: um dia que não rodou).
  - `full`: todos os dias desde o início dos dados (01/10/2026, `config/projeto.yaml`) até ontem.
- Em qualquer modo, o que já existe é pulado (coleta, envio e Bronze): o full só **preenche** os dias que faltarem.
- `lote` (opcional): rótulo gravado no log de contagens da Bronze.
"""


def criar_dag_fonte(nome: str) -> DAG:
    fonte = configuracao.fonte(nome)
    with DAG(
        dag_id=f"akona_{nome}",
        description=f"{fonte['descricao']}: API → chegada → Volume → Bronze → Prata",
        doc_md=_documentacao(fonte),
        schedule=None,                                  # quem agenda é a trigger-master
        start_date=pendulum.datetime(2026, 10, 1, tz=configuracao.projeto()["timezone"]),
        catchup=False,
        max_active_runs=1,
        params=copy.deepcopy(tarefas.PARAMETROS_FONTE),
        default_args={"owner": "akona-dados", "retries": 0, "on_failure_callback": avisar_falha},
        tags=["akona", "fonte", nome],
    ) as dag:
        inicio = EmptyOperator(task_id="inicio")
        coletar = PythonOperator(task_id="coletar_api", python_callable=tarefas.coletar,
                                 op_kwargs={"fonte": nome}, retries=2, retry_delay=timedelta(seconds=30))
        conferir = PythonOperator(task_id="conferir_chegada", python_callable=tarefas.conferir_chegada,
                                  op_kwargs={"fonte": nome})
        preparar = PythonOperator(task_id="preparar_databricks", python_callable=tarefas.preparar_databricks,
                                  op_kwargs={"pastas": [nome]}, retries=1)
        publicar = PythonOperator(task_id="publicar_notebooks", python_callable=tarefas.publicar_notebooks,
                                  op_kwargs={"notebooks": [NOTEBOOK_COMUM, NOTEBOOK_BRONZE, fonte["prata"]["notebook"]]},
                                  retries=1)
        enviar = PythonOperator(task_id="enviar_ao_volume", python_callable=tarefas.enviar_ao_volume,
                                op_kwargs={"fonte": nome}, retries=2, retry_delay=timedelta(seconds=30))
        bronze = PythonOperator(task_id="bronze", python_callable=tarefas.rodar_notebook,
                                op_kwargs={"notebook": NOTEBOOK_BRONZE, "nome": f"bronze_{nome}", "enviar_lote": True,
                                           "parametros": {"fonte": nome, "formato": fonte["bronze"]["formato"]}},
                                **tarefas.OPCOES_NOTEBOOK)
        prata = PythonOperator(task_id="prata", python_callable=tarefas.rodar_notebook,
                               op_kwargs={"notebook": fonte["prata"]["notebook"], "nome": f"prata_{nome}"},
                               **tarefas.OPCOES_NOTEBOOK)

        inicio >> [coletar, preparar, publicar]
        coletar >> conferir
        [conferir, preparar] >> enviar
        [enviar, publicar] >> bronze >> prata
    return dag
