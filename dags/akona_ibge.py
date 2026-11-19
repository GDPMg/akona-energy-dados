"""DAG do Airflow da fonte IBGE (município e Censo 2022): API -> chegada -> Volume -> Bronze -> Prata.

Tudo o que é específico desta fonte está em config/fontes/ibge.yaml.
A construção da DAG segue o padrão de include/akona/dag_fonte.py.
"""
from akona.dag_fonte import criar_dag_fonte

dag = criar_dag_fonte("ibge")
