"""DAG do Airflow da fonte ordens de serviço da manutenção da iluminação pública (simulada). Configuração em config/fontes/manutencao.yaml."""
from akona.dag_fonte import criar_dag_fonte

dag = criar_dag_fonte("manutencao")
