"""DAG da fonte chamados dos moradores (API do webapp Akona, simulada). Configuração em config/fontes/chamados.yaml."""
from akona.dag_fonte import criar_dag_fonte

dag = criar_dag_fonte("chamados")
