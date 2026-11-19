"""DAG do Airflow da fonte município e Censo 2022 (IBGE). Configuração em config/fontes/ibge.yaml."""
from akona.dag_fonte import criar_dag_fonte

dag = criar_dag_fonte("ibge")
