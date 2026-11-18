# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · 01 · Bronze (Auto Loader)
# MAGIC
# MAGIC Lê os arquivos que chegaram ao Volume e grava cada fonte numa tabela Bronze **sem transformar o dado**
# MAGIC (só acrescenta `arquivo_origem` e `carregado_em`). Tudo chega como texto; os tipos são definidos na Prata.
# MAGIC
# MAGIC | Fonte | Pasta no Volume | Tabela |
# MAGIC |---|---|---|
# MAGIC | Chamados (API Akona, simulada) | `chegada/chamados/` | `bronze_chamados` (1 linha por registro do JSON Lines) |
# MAGIC | Clima (Open-Meteo) | `chegada/clima/` | `bronze_clima` (1 linha por arquivo/dia) |
# MAGIC | IBGE | `chegada/ibge/` | `bronze_ibge` (1 linha por arquivo/dia) |
# MAGIC
# MAGIC - **Incremental:** o Auto Loader guarda no *checkpoint* quais arquivos já leu. No 2º lote só os arquivos novos entram.
# MAGIC - **Sem duplicar:** rodar de novo sem arquivos novos não insere nada (`linhas_novas = 0`).
# MAGIC - Antes de rodar, escolha no widget **lote** o rótulo desta execução. Ele aparece no log de contagens (print do relatório).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.dropdown("lote", "lote_1", ["lote_1", "lote_2", "reexecucao", "outro"], "Rótulo desta execução")
LOTE = dbutils.widgets.get("lote")

BASE = "/Volumes/workspace/akona/chegada"
FONTES = ["chamados", "clima", "ibge"]

# COMMAND ----------

def contar(tabela: str) -> tuple:
    """(linhas, arquivos distintos) da tabela; (0, 0) se ela ainda não existe."""
    if not spark.catalog.tableExists(tabela):
        return 0, 0
    r = spark.table(tabela).agg(F.count("*").alias("linhas"), F.countDistinct("arquivo_origem").alias("arquivos")).first()
    return r["linhas"], r["arquivos"]


def tem_arquivos(fonte: str) -> bool:
    try:
        return any(a.name.endswith(".json") for a in dbutils.fs.ls(f"{BASE}/{fonte}"))
    except Exception:
        return False


def carregar_bronze(fonte: str) -> dict:
    """Auto Loader: lê só os arquivos novos da pasta da fonte e acrescenta na tabela Bronze."""
    tabela = f"workspace.akona.bronze_{fonte}"
    linhas_antes, arquivos_antes = contar(tabela)

    if tem_arquivos(fonte):
        consulta = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
            .option("pathGlobFilter", "*.json")               # ignora .tmp e outros arquivos
            .load(f"{BASE}/{fonte}")
            .withColumn("arquivo_origem", F.col("_metadata.file_path"))
            .withColumn("carregado_em", F.current_timestamp())
            .writeStream
            .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
            .trigger(availableNow=True)                       # processa o que é novo e para
            .toTable(tabela)
        )
        consulta.awaitTermination()
    else:
        print(f"Aviso: nenhum arquivo .json em {BASE}/{fonte}. Envie o lote antes de rodar.")

    linhas_depois, arquivos_depois = contar(tabela)
    return {
        "lote": LOTE, "fonte": fonte, "tabela": tabela,
        "linhas_antes": linhas_antes, "linhas_depois": linhas_depois,
        "linhas_novas": linhas_depois - linhas_antes,
        "arquivos_antes": arquivos_antes, "arquivos_depois": arquivos_depois,
    }


resultados = [carregar_bronze(fonte) for fonte in FONTES]

log = (
    spark.createDataFrame(resultados)
    .withColumn("execucao_em", F.current_timestamp())
    .select("execucao_em", "lote", "fonte", "tabela", "linhas_antes", "linhas_depois",
            "linhas_novas", "arquivos_antes", "arquivos_depois")
)
log.write.mode("append").saveAsTable("workspace.akona.controle_cargas_bronze")
display(log)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contagens antes e depois de cada execução (print do requisito 05 e da seção 07 do relatório)
# MAGIC Esperado: `lote_1` sai de 0 para N₁; `lote_2` sai de N₁ para N₁ + N₂; `reexecucao` (rodar de novo sem arquivos novos) mantém N₁ + N₂ e `linhas_novas = 0`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT execucao_em, lote, fonte, linhas_antes, linhas_depois, linhas_novas, arquivos_antes, arquivos_depois
# MAGIC FROM workspace.akona.controle_cargas_bronze
# MAGIC ORDER BY execucao_em, fonte;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prova de que nenhum arquivo entrou duas vezes
# MAGIC Cada arquivo deve ter sido carregado em **uma única** execução. Resultado vazio = sem duplicação.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fonte, arquivo_origem, COUNT(DISTINCT carregado_em) AS cargas
# MAGIC FROM (
# MAGIC   SELECT 'chamados' AS fonte, arquivo_origem, carregado_em FROM workspace.akona.bronze_chamados
# MAGIC   UNION ALL SELECT 'clima', arquivo_origem, carregado_em FROM workspace.akona.bronze_clima
# MAGIC   UNION ALL SELECT 'ibge', arquivo_origem, carregado_em FROM workspace.akona.bronze_ibge
# MAGIC )
# MAGIC GROUP BY fonte, arquivo_origem
# MAGIC HAVING COUNT(DISTINCT carregado_em) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arquivos por fonte e por mês (mostra os dois lotes)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fonte, regexp_extract(arquivo_origem, '(\\d{4}-\\d{2})-\\d{2}\\.json$', 1) AS mes_coleta,
# MAGIC        COUNT(DISTINCT arquivo_origem) AS arquivos, COUNT(*) AS linhas
# MAGIC FROM (
# MAGIC   SELECT 'chamados' AS fonte, arquivo_origem FROM workspace.akona.bronze_chamados
# MAGIC   UNION ALL SELECT 'clima', arquivo_origem FROM workspace.akona.bronze_clima
# MAGIC   UNION ALL SELECT 'ibge', arquivo_origem FROM workspace.akona.bronze_ibge
# MAGIC )
# MAGIC GROUP BY ALL
# MAGIC ORDER BY fonte, mes_coleta;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histórico da tabela (cada lote aparece como um STREAMING UPDATE)

# COMMAND ----------

display(
    spark.sql("DESCRIBE HISTORY workspace.akona.bronze_chamados")
    .select("version", "timestamp", "operation",
            F.col("operationMetrics").getItem("numOutputRows").alias("linhas_gravadas"))
)
