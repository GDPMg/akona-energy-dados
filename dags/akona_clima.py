"""DAG do Airflow da fonte clima horário de Osasco (Open-Meteo). Configuração em config/fontes/clima.yaml."""
from akona.dag_fonte import criar_dag_fonte

dag = criar_dag_fonte("clima")
