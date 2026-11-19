# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · Prata · manutenção (ordens de serviço da iluminação pública)
# MAGIC
# MAGIC `bronze_manutencao` (texto bruto) → `prata_manutencao` (1 linha por ordem de serviço, último estado).
# MAGIC Executado pela DAG `akona_manutencao`. Feito a partir do modelo `prata/_modelo_prata.py`.
# MAGIC
# MAGIC Alimenta a tela **Painel da gestão** do MVP: o **ponto** (poste) de cada chamado e o indicador de **reincidência**.
# MAGIC Não há dado pessoal nesta fonte: as equipes são identificadas só por código.

# COMMAND ----------

# MAGIC %run ../comum/funcoes

# COMMAND ----------

TABELA = "prata_manutencao"
CHAVES = ["os_id"]
STATUS_OS = ["aguardando_equipe", "em_execucao", "concluida"]
PADRAO_PONTO = r"^OSC-\d{5}$"               # código do poste: OSC + bairro (2 dígitos) + número (3 dígitos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tipos, normalização e regras de qualidade
# MAGIC | Regra | Barra o registro quando |
# MAGIC |---|---|
# MAGIC | M1_obrigatorios | ordem de serviço, chamado, poste ou data de abertura nulos |
# MAGIC | M2_dominio | status fora de aguardando_equipe / em_execucao / concluida, ou poste fora do padrão `OSC-NNNNN` |
# MAGIC | M3_datas_coerentes | conclusão anterior à abertura |

# COMMAND ----------

tratado = (
    spark.table(f"{S}.bronze_manutencao")
    .withColumn("status", chave(F.col("status")))
    .withColumn("ponto_id", F.upper(F.trim("ponto_id")))
    .withColumn("bairro_chave", chave(F.col("bairro")))
    .join(F.broadcast(referencia_bairros()), "bairro_chave", "left")
    .withColumn("potencia_w", F.expr("try_cast(potencia_w AS INT)"))
    .withColumn("aberta_em", para_timestamp("aberta_em"))
    .withColumn("iniciada_em", para_timestamp("iniciada_em"))
    .withColumn("concluida_em", para_timestamp("concluida_em"))
    .withColumn("atualizado_em", para_timestamp("atualizado_em"))
)

REGRAS = [
    ("M1_obrigatorios", "ordem de serviço, chamado, poste e abertura preenchidos",
     F.col("os_id").isNull() | F.col("id_chamado").isNull() | F.col("ponto_id").isNull() | F.col("aberta_em").isNull()),
    ("M2_dominio", "status aceito e poste no padrão OSC-NNNNN",
     F.coalesce(~F.col("status").isin(STATUS_OS), F.lit(True))
     | F.coalesce(~F.col("ponto_id").rlike(PADRAO_PONTO), F.lit(False))),
    ("M3_datas_coerentes", "conclusão não pode ser anterior à abertura",
     F.coalesce(F.col("concluida_em") < F.col("aberta_em"), F.lit(False))),
]
avaliado = aplicar_regras(tratado, REGRAS)
display(registrar_qualidade(TABELA, REGRAS, avaliado))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplicação e MERGE pela chave `os_id`
# MAGIC Cada dia traz OS novas e mudanças de estado (aguardando equipe → em execução → concluída):
# MAGIC o MERGE só atualiza quando o registro que chegou é mais recente.

# COMMAND ----------

lote = ultimo_por_chave(
    avaliado.filter(~F.col("reprovado")), CHAVES,
    [F.col("atualizado_em").desc_nulls_last(), F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select(
    "os_id", "id_chamado", "ponto_id",
    F.col("bairro_oficial").alias("bairro"),
    F.lower("tecnologia").alias("tecnologia"), "potencia_w",
    "tipo_problema", "tipo_servico", F.upper("equipe").alias("equipe"), "status",
    "aberta_em", "iniciada_em", "concluida_em",
    F.coalesce("atualizado_em", "aberta_em").alias("atualizado_em"),
    "arquivo_origem", F.current_timestamp().alias("processado_em"),
)

garantir_tabela(f"{S}.{TABELA}", f"""
CREATE TABLE IF NOT EXISTS {S}.{TABELA} (
  os_id STRING NOT NULL COMMENT 'Ordem de serviço (OS-AAAAMMDD-TT-NNN)',
  id_chamado STRING COMMENT 'Chamado que originou a OS',
  ponto_id STRING COMMENT 'Ponto de iluminação (poste) atendido, ex.: OSC-03024',
  bairro STRING, tecnologia STRING, potencia_w INT,
  tipo_problema STRING, tipo_servico STRING, equipe STRING, status STRING,
  aberta_em TIMESTAMP, iniciada_em TIMESTAMP, concluida_em TIMESTAMP, atualizado_em TIMESTAMP,
  arquivo_origem STRING, processado_em TIMESTAMP
) COMMENT 'Prata: ordens de serviço da manutenção da iluminação pública (último estado de cada OS). Sem dados pessoais.'""")

display(merge_por_chave(lote, TABELA, CHAVES, "atualizado_em"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resultados para o relatório

# COMMAND ----------

display(ultimo_resumo_qualidade(TABELA))

# COMMAND ----------

display(spark.table(f"{S}.{TABELA}").groupBy("status").count().orderBy(F.desc("count")))

# COMMAND ----------

display(historico_merge(TABELA))
