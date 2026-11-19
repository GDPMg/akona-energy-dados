"""
### Akona Energy · consultas de análise

Executa em paralelo os notebooks de `config/consultas.yaml`; o link de cada resultado fica no log da tarefa.
"""
from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from akona import configuracao, tarefas
from akona.notificacao import avisar_falha

NOTEBOOKS = configuracao.consultas()

with DAG(
    dag_id="akona_consultas",
    description="Akona Energy: consultas de análise do MVP e verificação de LGPD no Databricks",
    doc_md=__doc__,
    schedule=None,                         # disparada pela trigger-master
    start_date=pendulum.datetime(2026, 10, 1, tz=configuracao.projeto()["timezone"]),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "akona-dados", "retries": 0, "on_failure_callback": avisar_falha},
    tags=["akona", "consultas"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    publicar = PythonOperator(task_id="publicar_notebooks", python_callable=tarefas.publicar_notebooks,
                              op_kwargs={"notebooks": [f"consultas/{n['nome']}" for n in NOTEBOOKS]}, retries=1)
    inicio >> publicar
    for notebook in NOTEBOOKS:
        publicar >> PythonOperator(
            task_id=notebook["nome"], python_callable=tarefas.rodar_notebook,
            op_kwargs={"notebook": f"consultas/{notebook['nome']}", "nome": notebook["nome"]},
            doc_md=notebook.get("descricao"), **tarefas.OPCOES_NOTEBOOK)
