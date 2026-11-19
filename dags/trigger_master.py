"""
### Akona Energy · **trigger-master** (orquestrador)

Dispara todas as outras DAGs na ordem certa e espera cada uma terminar:

1. as DAGs de fonte (`akona_<fonte>`), respeitando `depende_de` de `config/fontes/*.yaml`
   (hoje: `akona_clima` ∥ `akona_ibge` em paralelo → `akona_chamados`, que usa o clima do dia);
2. `akona_ouro` (dimensões, fato e tabela do MVP), quando todas as fontes chegaram à Prata;
3. `akona_consultas` (análises do MVP e verificação de LGPD).

Uma fonte nova cadastrada em `config/fontes/` entra aqui automaticamente.

**Agenda:** todo dia às 06:00. As DAGs de fonte rodam no modo padrão, **incremental** (coletam o dia anterior).
A execução agendada só acontece se a Variable `akona_execucao_diaria` for `true`; execução manual (Trigger) sempre roda.

**Data específica ou full:** o parâmetro `modo` fica em cada DAG de fonte. Dispare `akona_clima`, `akona_ibge` e
`akona_chamados` com o modo desejado e depois `akona_ouro` e `akona_consultas` (ver README).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Variable

from akona import configuracao
from akona.notificacao import avisar_falha, enviar_notificacao

FONTES = configuracao.fontes()
TZ = configuracao.projeto()["timezone"]


def verificar_execucao(**context) -> bool:
    """Execução manual sempre roda; a agendada depende da Variable akona_execucao_diaria."""
    if "scheduled" not in str(context["dag_run"].run_type).lower():
        return True
    return str(Variable.get("akona_execucao_diaria", default="true")).strip().lower() == "true"


def notificar(**context) -> None:
    ontem = (datetime.now(ZoneInfo(TZ)) - timedelta(days=1)).date().isoformat()
    enviar_notificacao(
        f"✅ Akona Energy · trigger-master concluída (incremental, dia {ontem}). "
        f"Fontes: {', '.join(FONTES)} → Ouro → consultas.")


def disparar(dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=dag_id,
        trigger_dag_id=dag_id,
        trigger_run_id="{{ run_id }}",     # a execução filha recebe o mesmo nome da execução da master
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=10,
    )


with DAG(
    dag_id="trigger-master",
    description="Akona Energy: orquestra as DAGs de fonte, a Ouro e as consultas, na ordem certa",
    doc_md=__doc__,
    schedule="0 6 * * *",                  # todo dia às 06:00 (as fontes coletam o dia anterior)
    start_date=pendulum.datetime(2026, 11, 18, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "akona-dados", "retries": 0, "on_failure_callback": avisar_falha},
    tags=["akona", "orquestrador"],
) as dag:
    verificar = ShortCircuitOperator(task_id="verificar_execucao", python_callable=verificar_execucao)
    fontes = {nome: disparar(f"akona_{nome}") for nome in FONTES}
    ouro = disparar("akona_ouro")
    consultas = disparar("akona_consultas")
    aviso = PythonOperator(task_id="notificar", python_callable=notificar)

    for nome, fonte in FONTES.items():
        if fonte["depende_de"]:
            [fontes[d] for d in fonte["depende_de"]] >> fontes[nome]
        else:
            verificar >> fontes[nome]
    list(fontes.values()) >> ouro >> consultas >> aviso
