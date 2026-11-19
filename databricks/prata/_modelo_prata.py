# Databricks notebook source
# MAGIC %md
# MAGIC # MODELO de Prata para uma fonte nova (copie este arquivo como `prata_<fonte>.py`)
# MAGIC
# MAGIC Passos (ver `docs/como_adicionar_fonte.md`):
# MAGIC 1. Troque `FONTE`, `TABELA` e `CHAVES` abaixo.
# MAGIC 2. Em **Tipos e tratamento**, converta os campos da Bronze (tudo texto) para os tipos certos.
# MAGIC 3. Em **Regras**, escreva pelo menos uma regra de qualidade: (nome, descrição, condição que VIOLA a regra).
# MAGIC 4. Ajuste o `CREATE TABLE` com as colunas finais.
# MAGIC
# MAGIC Bronze, normalização, contagem das regras, deduplicação e MERGE já vêm prontos de `comum/funcoes`.

# COMMAND ----------

# MAGIC %run ../comum/funcoes

# COMMAND ----------

FONTE = "minha_fonte"                 # mesmo nome do config/fontes/<fonte>.yaml
TABELA = "prata_minha_fonte"          # tabela de destino na Prata
CHAVES = ["id"]                       # chave do MERGE

# COMMAND ----------

# Tipos e tratamento: a Bronze guarda tudo como texto (JSON aninhado também). Use try_cast / para_timestamp / from_json.
tratado = (
    spark.table(f"{S}.bronze_{FONTE}")
    .withColumn("data", para_timestamp("data", ("yyyy-MM-dd",)))
    .withColumn("valor", F.expr("try_cast(valor AS DOUBLE)"))
)

# Regras de qualidade: cada uma é (nome, descrição, condição que VIOLA a regra)
REGRAS = [
    ("Q1_chave_preenchida", "a chave não pode ser nula", F.col("id").isNull()),
    ("Q2_valor_valido", "valor numérico e não negativo", fora("valor", 0, 1e12)),
]
avaliado = aplicar_regras(tratado, REGRAS)
display(registrar_qualidade(TABELA, REGRAS, avaliado))

lote = ultimo_por_chave(
    avaliado.filter(~F.col("reprovado")), CHAVES, [F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select(*CHAVES, "data", "valor", "arquivo_origem", "carregado_em", F.current_timestamp().alias("processado_em"))

garantir_tabela(f"{S}.{TABELA}", f"""
CREATE TABLE IF NOT EXISTS {S}.{TABELA} (
  id STRING NOT NULL, data TIMESTAMP, valor DOUBLE,
  arquivo_origem STRING, carregado_em TIMESTAMP, processado_em TIMESTAMP
) COMMENT 'Prata: descreva a tabela'""")

display(merge_por_chave(lote, TABELA, CHAVES, "carregado_em"))
