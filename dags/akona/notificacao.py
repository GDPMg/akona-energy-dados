"""Notificações dos DAGs do Akona Energy: log da tarefa e, se configurado, webhook do Discord."""
from __future__ import annotations

import logging

import requests
from airflow.sdk import Variable

log = logging.getLogger(__name__)


def enviar_notificacao(texto: str) -> None:
    """Registra a notificação no log e, se a Variable akona_webhook_secret existir, envia ao Discord."""
    log.info("NOTIFICAÇÃO: %s", texto)
    webhook = Variable.get("akona_webhook_secret", default="")
    if not webhook:
        return
    try:
        requests.post(webhook, json={"content": texto[:1900]}, timeout=15).raise_for_status()
    except requests.RequestException as erro:
        log.warning("Não foi possível enviar a notificação ao webhook: %s", erro)


def avisar_falha(context) -> None:
    """Callback de falha: avisa qual tarefa falhou (depois de esgotar as tentativas)."""
    ti = context["ti"]
    enviar_notificacao(
        f"❌ Akona Energy · a tarefa '{ti.task_id}' do DAG '{ti.dag_id}' falhou na execução "
        f"{context.get('run_id')}. Veja o log no Airflow."
    )
