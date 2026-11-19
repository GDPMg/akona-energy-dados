# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · Bronze genérica (Auto Loader)
# MAGIC
# MAGIC **Um único notebook para todas as fontes.** A DAG `akona_<fonte>` do Airflow executa este notebook com os
# MAGIC parâmetros `fonte`, `formato` e `lote`. Uma fonte nova não precisa de notebook de Bronze.
# MAGIC
# MAGIC | Parâmetro | Exemplo | Uso |
# MAGIC |---|---|---|
# MAGIC | `fonte` | `ibge` | lê `chegada/ibge/` no Volume e grava `bronze_ibge` |
# MAGIC | `formato` | `json` | formato dos arquivos (Auto Loader) |
# MAGIC | `lote` | `lote_2` | rótulo gravado no log de contagens |
# MAGIC
# MAGIC - **Dado bruto:** nada é transformado; só entram as colunas de linhagem `arquivo_origem` e `carregado_em`
# MAGIC   (campos aninhados do JSON ficam como texto e a Prata aplica o schema).
# MAGIC - **Incremental:** o Auto Loader guarda no *checkpoint* quais arquivos já leu. No 2º lote só os novos entram.
# MAGIC - **Sem duplicar:** rodar de novo sem arquivos novos não insere nada (`linhas_novas = 0`).

# COMMAND ----------

# MAGIC %run ../comum/funcoes

# COMMAND ----------

dbutils.widgets.text("fonte", "", "Fonte (pasta no Volume)")
dbutils.widgets.text("formato", "json", "Formato dos arquivos")
dbutils.widgets.text("lote", "manual", "Rótulo desta execução")
FONTE = dbutils.widgets.get("fonte").strip()
FORMATO = dbutils.widgets.get("formato").strip() or "json"
LOTE = dbutils.widgets.get("lote").strip() or "manual"
if not FONTE:
    raise ValueError("Informe o parâmetro 'fonte' (ex.: clima, ibge, chamados).")

PASTA = f"{BASE_VOLUME}/{FONTE}"
TABELA = f"{S}.bronze_{FONTE}"

# COMMAND ----------

def contar(tabela: str) -> tuple:
    """(linhas, arquivos distintos) da tabela; (0, 0) se ela ainda não existe."""
    if not spark.catalog.tableExists(tabela):
        return 0, 0
    r = spark.table(tabela).agg(F.count("*").alias("linhas"), F.countDistinct("arquivo_origem").alias("arquivos")).first()
    return r["linhas"], r["arquivos"]


def tem_arquivos(pasta: str) -> bool:
    try:
        return any(not a.name.endswith(".tmp") for a in dbutils.fs.ls(pasta))
    except Exception:
        return False


linhas_antes, arquivos_antes = contar(TABELA)

if tem_arquivos(PASTA):
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", FORMATO)
        .option("cloudFiles.schemaLocation", f"{BASE_VOLUME}/_controle/schema_{FONTE}")
        .option("pathGlobFilter", f"*.{FORMATO}")          # ignora .tmp e outros arquivos
        .load(PASTA)
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE_VOLUME}/_controle/checkpoint_{FONTE}")
        .trigger(availableNow=True)                         # processa o que é novo e para
        .toTable(TABELA)
        .awaitTermination()
    )
else:
    print(f"Aviso: nenhum arquivo em {PASTA}. Nada a carregar.")

linhas_depois, arquivos_depois = contar(TABELA)
log = (
    spark.createDataFrame(
        [(LOTE, FONTE, TABELA, linhas_antes, linhas_depois, linhas_depois - linhas_antes, arquivos_antes, arquivos_depois)],
        "lote STRING, fonte STRING, tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT, linhas_novas BIGINT, "
        "arquivos_antes BIGINT, arquivos_depois BIGINT")
    .withColumn("execucao_em", F.current_timestamp())
    .select("execucao_em", "lote", "fonte", "tabela", "linhas_antes", "linhas_depois",
            "linhas_novas", "arquivos_antes", "arquivos_depois")
)
log.write.mode("append").saveAsTable(f"{S}.controle_cargas_bronze")
display(log)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contagens antes e depois de cada execução desta fonte (print do requisito 05)
# MAGIC Esperado: `lote_1` sai de 0 para N₁; `lote_2` sai de N₁ para N₁ + N₂; `reexecucao` mantém N₁ + N₂ (`linhas_novas = 0`).

# COMMAND ----------

display(spark.table(f"{S}.controle_cargas_bronze").filter(F.col("fonte") == FONTE).orderBy("execucao_em"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prova de que nenhum arquivo entrou duas vezes (resultado vazio = sem duplicação)

# COMMAND ----------

display(
    spark.table(TABELA).groupBy("arquivo_origem")
    .agg(F.countDistinct("carregado_em").alias("cargas"))
    .filter("cargas > 1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arquivos e linhas por mês de coleta (mostra os lotes)

# COMMAND ----------

display(
    spark.table(TABELA)
    .withColumn("mes_coleta", F.regexp_extract("arquivo_origem", r"(\d{4}-\d{2})-\d{2}\.\w+$", 1))
    .groupBy("mes_coleta")
    .agg(F.countDistinct("arquivo_origem").alias("arquivos"), F.count("*").alias("linhas"))
    .orderBy("mes_coleta")
)
