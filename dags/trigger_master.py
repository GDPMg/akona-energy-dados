"""
### Akona Energy · **trigger-master** (orquestrador)

Dispara todas as outras DAGs na ordem certa e espera cada uma terminar:

1. as DAGs de fonte (`akona_<fonte>`), respeitando `depende_de` de `config/fontes/*.yaml`
   (hoje: `akona_clima` ∥ `akona_ibge` em paralelo → `akona_chamados`, que usa o clima do dia);
2. `akona_ouro` (dimensões, fato e tabela do MVP), quando todas as fontes chegaram à Prata;
3. `akona_consultas` (análises do MVP e verificação de LGPD).

Uma fonte nova cadastrada em `config/fontes/` entra aqui automaticamente.

**Agenda:** todo dia às 06:00, coletando o dia anterior. A execução agendada só acontece se a Variable
`akona_execucao_diaria` for `true`; execuções manuais (Trigger) sempre rodam.
**Parâmetros:** `de`, `ate`, `lote` e `recoletar`, repassados a todas as DAGs (vazio = dia anterior).
Ex.: lote 1 = `de` 2026-10-01, `ate` 2026-10-31, `lote` lote_1.
"""
from __future__ import annotations

import copy

import pendulum
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Variable

from akona import configuracao, tarefas
from akona.notificacao import avisar_falha, enviar_notificacao

FONTES = configuracao.fontes()


def verificar_execucao(**context) -> bool:
    """Execução manual sempre roda; a agendada depende da Variable akona_execucao_diaria."""
    if "scheduled" not in str(context["dag_run"].run_type).lower():
        return True
    return str(Variable.get("akona_execucao_diaria", default="true")).strip().lower() == "true"


def notificar(**context) -> None:
    periodo = context["ti"].xcom_pull(task_ids="definir_periodo") or {}
    enviar_notificacao(
        f"✅ Akona Energy · trigger-master concluída ({periodo.get('lote')}: {periodo.get('de')} a {periodo.get('ate')}). "
        f"Fontes: {', '.join(FONTES)} → Ouro → consultas.")


def disparar(dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=dag_id,
        trigger_dag_id=dag_id,
        conf="{{ ti.xcom_pull(task_ids='definir_periodo') }}",   # de, ate, lote e recoletar
        trigger_run_id="{{ run_id }}",                            # a execução filha recebe o mesmo nome
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=10,
    )


with DAG(
    dag_id="trigger-master",
    description="Akona Energy: orquestra as DAGs de fonte, a Ouro e as consultas, na ordem certa",
    doc_md=__doc__,
    schedule="0 6 * * *",                  # todo dia às 06:00 (coleta o dia anterior)
    start_date=pendulum.datetime(2026, 11, 18, tz=configuracao.projeto()["timezone"]),
    catchup=False,
    max_active_runs=1,
    params=copy.deepcopy(tarefas.PARAMETROS_PERIODO),
    render_template_as_native_obj=True,    # conf repassado como dicionário, com os tipos certos
    default_args={"owner": "akona-dados", "retries": 0, "on_failure_callback": avisar_falha},
    tags=["akona", "orquestrador"],
) as dag:
    verificar = ShortCircuitOperator(task_id="verificar_execucao", python_callable=verificar_execucao)
    periodo = PythonOperator(task_id="definir_periodo", python_callable=tarefas.definir_periodo)
    fontes = {nome: disparar(f"akona_{nome}") for nome in FONTES}
    ouro = disparar("akona_ouro")
    consultas = disparar("akona_consultas")
    aviso = PythonOperator(task_id="notificar", python_callable=notificar)

    verificar >> periodo
    for nome, fonte in FONTES.items():
        if fonte["depende_de"]:
            [fontes[d] for d in fonte["depende_de"]] >> fontes[nome]
        else:
            periodo >> fontes[nome]
    list(fontes.values()) >> ouro >> consultas >> aviso
