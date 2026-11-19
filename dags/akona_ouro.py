"""
### Akona Energy · camada **Ouro**

Uma tarefa por tabela ou view de `config/ouro.yaml` (`databricks/ouro/<nome>.sql`); `depende_de` define a ordem.
"""
from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from akona import configuracao, tarefas
from akona.notificacao import avisar_falha

TABELAS = configuracao.ouro()

with DAG(
    dag_id="akona_ouro",
    description="Akona Energy: camada Ouro (dimensões, fato e tabela-contrato do MVP) no Databricks",
    doc_md=__doc__,
    schedule=None,                         # disparada pela trigger-master
    start_date=pendulum.datetime(2026, 10, 1, tz=configuracao.projeto()["timezone"]),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,                    # o Databricks Free aceita até 5 tarefas simultâneas
    default_args={"owner": "akona-dados", "retries": 0, "on_failure_callback": avisar_falha},
    tags=["akona", "ouro"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    preparar = PythonOperator(task_id="preparar_databricks", python_callable=tarefas.preparar_databricks,
                              op_kwargs={"segredo": True}, retries=1)
    publicar = PythonOperator(task_id="publicar_notebooks", python_callable=tarefas.publicar_notebooks,
                              op_kwargs={"notebooks": [f"ouro/{t['nome']}" for t in TABELAS]}, retries=1)
    inicio >> [preparar, publicar]

    tarefas_ouro = {
        t["nome"]: PythonOperator(
            task_id=t["nome"], python_callable=tarefas.rodar_notebook,
            op_kwargs={"notebook": f"ouro/{t['nome']}", "nome": f"ouro_{t['nome']}"},
            **tarefas.OPCOES_NOTEBOOK)
        for t in TABELAS
    }
    for t in TABELAS:
        if t["depende_de"]:
            [tarefas_ouro[d] for d in t["depende_de"]] >> tarefas_ouro[t["nome"]]
        else:
            [preparar, publicar] >> tarefas_ouro[t["nome"]]
