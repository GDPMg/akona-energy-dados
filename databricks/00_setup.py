# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · 00 · Setup do lakehouse
# MAGIC
# MAGIC Cria a estrutura no Unity Catalog do Databricks Free Edition:
# MAGIC
# MAGIC | Objeto | Para quê |
# MAGIC |---|---|
# MAGIC | schema `workspace.akona` | todas as tabelas do projeto (`bronze_*`, `prata_*`, `ouro_*` e `controle_*`) |
# MAGIC | volume `workspace.akona.chegada` | pasta de pouso: recebe, **por upload manual em lotes**, os arquivos da pasta `chegada/` do Airflow |
# MAGIC | pastas `chamados/`, `clima/`, `ibge/` | uma por fonte: é para elas que cada lote é enviado |
# MAGIC | pasta `_controle/` | schema inferido e checkpoint do Auto Loader |
# MAGIC | `controle_cargas_bronze` e `controle_qualidade` | logs usados nos prints do relatório |
# MAGIC
# MAGIC Rode **uma vez**, antes do primeiro upload.

# COMMAND ----------

CATALOGO, SCHEMA, VOLUME = "workspace", "akona", "chegada"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA} "
          "COMMENT 'Akona Energy: pipeline de dados da GS FIAP 2026.2 (CitizenTech)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Pasta de pouso: lotes enviados a partir da pasta chegada/ do Airflow'")

for pasta in ["chamados", "clima", "ibge", "_controle"]:
    dbutils.fs.mkdirs(f"{BASE}/{pasta}")

display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.akona.controle_cargas_bronze (
# MAGIC   execucao_em TIMESTAMP, lote STRING, fonte STRING, tabela STRING,
# MAGIC   linhas_antes BIGINT, linhas_depois BIGINT, linhas_novas BIGINT,
# MAGIC   arquivos_antes BIGINT, arquivos_depois BIGINT
# MAGIC ) COMMENT 'Log de cada execução da Bronze: contagens antes e depois (requisito 05)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.akona.controle_qualidade (
# MAGIC   execucao_em TIMESTAMP, tabela STRING, regra STRING, descricao STRING,
# MAGIC   total_lido BIGINT, barrados BIGINT
# MAGIC ) COMMENT 'Log das regras de qualidade da Prata: total barrado por regra (requisito 06)';
# MAGIC
# MAGIC SHOW TABLES IN workspace.akona;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Próximo passo: enviar o **lote 1**
# MAGIC 1. Menu **Catalog** → `workspace` → `akona` → **Volumes** → `chegada`.
# MAGIC 2. Abra a pasta `chamados/` → **Upload to this volume** → selecione os arquivos `chamados_2026-10-*.json` da pasta `chegada/chamados` do projeto.
# MAGIC 3. Repita para `clima/` (arquivos `clima_2026-10-*.json`) e `ibge/` (arquivos `ibge_2026-10-*.json`).
# MAGIC 4. Rode o notebook **01_bronze_autoloader** com o widget **lote = lote_1**.
