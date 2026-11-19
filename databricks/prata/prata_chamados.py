# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · Prata · chamados
# MAGIC
# MAGIC `bronze_chamados` (texto bruto) → `prata_chamados` (tratado e validado, 1 linha por chamado). Executado pela DAG `akona_chamados`.
# MAGIC
# MAGIC 1. **Tipos corretos:** datas viram `TIMESTAMP`, coordenadas `DOUBLE` e domicílios `INT` (`try_cast`/`try_to_timestamp`).
# MAGIC 2. **Nulos e inválidos tratados:** tipo, status e bairro normalizados; datas em 2 formatos; domicílios nulos recebem a mediana do tipo; canal nulo vira `nao_informado`.
# MAGIC 3. **Regras de qualidade:** quem viola uma regra vai para `prata_chamados_rejeitados`; o total barrado por regra fica em `controle_qualidade`.
# MAGIC 4. **MERGE pela chave `id_chamado`:** só atualiza quando o registro que chegou é mais recente (mudança de status).

# COMMAND ----------

# MAGIC %run ../comum/funcoes

# COMMAND ----------

TIPOS = ["fio_caido", "queda_energia", "poste_apagado", "lampada_piscando"]
STATUS = ["aberto", "em_andamento", "resolvido"]
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = -23.58, -23.47, -46.84, -46.73   # retângulo que contém Osasco

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tipos, normalização e tratamento de nulos

# COMMAND ----------

ref_bairros = referencia_bairros()          # lista oficial de bairros (comum/funcoes)

tratado = (
    spark.table(f"{S}.bronze_chamados")
    .withColumn("tipo_bruto", F.col("tipo_problema"))
    .withColumn("status_bruto", F.col("status"))
    .withColumn("tipo_problema", chave(F.col("tipo_problema")))
    .withColumn("status", chave(F.col("status")))
    .withColumn("bairro_chave", chave(F.col("bairro")))
    .join(F.broadcast(ref_bairros), "bairro_chave", "left")          # bairro fora da lista -> NULL
    .withColumn("aberto_em", para_timestamp("aberto_em"))
    .withColumn("atualizado_em", para_timestamp("atualizado_em"))
    .withColumn("resolvido_em", para_timestamp("resolvido_em"))
    .withColumn("latitude", F.expr("try_cast(latitude AS DOUBLE)"))
    .withColumn("longitude", F.expr("try_cast(longitude AS DOUBLE)"))
    .withColumn("domicilios_afetados", F.expr("try_cast(domicilios_afetados AS INT)"))
    .withColumn("canal", F.coalesce(chave(F.col("canal")), F.lit("nao_informado")))
    .withColumn("cpf", F.regexp_replace("cidadao_cpf", "[^0-9]", ""))
)

# "De -> para" da normalização do tipo (bom print para o relatório)
display(tratado.groupBy("tipo_bruto", "tipo_problema").count().orderBy("tipo_problema", F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras de qualidade (total barrado por regra)
# MAGIC | Regra | Barra o registro quando |
# MAGIC |---|---|
# MAGIC | R1_obrigatorios | id, tipo ou data de abertura está nulo ou ilegível |
# MAGIC | R2_dominio | tipo fora dos 4 aceitos ou status fora de aberto / em_andamento / resolvido |
# MAGIC | R3_localizacao | bairro não é de Osasco ou latitude/longitude fora do município (ex.: GPS 0,0) |
# MAGIC | R4_datas_coerentes | data de resolução anterior à data de abertura |

# COMMAND ----------

REGRAS = [
    ("R1_obrigatorios", "id, tipo e data de abertura preenchidos e legíveis",
     F.col("id_chamado").isNull() | F.col("tipo_problema").isNull() | F.col("aberto_em").isNull()),
    ("R2_dominio", "tipo e status dentro dos valores aceitos",
     F.coalesce(~F.col("tipo_problema").isin(TIPOS), F.lit(False))
     | F.coalesce(~F.col("status").isin(STATUS), F.lit(True))),
    ("R3_localizacao", "bairro de Osasco e coordenadas dentro do município",
     F.col("bairro_oficial").isNull() | fora("latitude", LAT_MIN, LAT_MAX) | fora("longitude", LON_MIN, LON_MAX)),
    ("R4_datas_coerentes", "resolução não pode ser anterior à abertura",
     F.coalesce(F.col("resolvido_em") < F.col("aberto_em"), F.lit(False))),
]

chamados = aplicar_regras(tratado, REGRAS)
display(registrar_qualidade("prata_chamados", REGRAS, chamados))

# Quarentena: registros barrados, com o motivo (sem dados pessoais)
(
    chamados.filter("reprovado")
    .withColumn("motivos", F.concat_ws(", ", *[F.when(F.col(n), F.lit(n)) for n, _, _ in REGRAS]))
    .select("id_chamado", "motivos", "tipo_bruto", "status_bruto", F.col("bairro").alias("bairro_bruto"),
            "latitude", "longitude", "aberto_em", "resolvido_em", "arquivo_origem")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.prata_chamados_rejeitados")
)
display(spark.table(f"{S}.prata_chamados_rejeitados").groupBy("motivos").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicação e MERGE pela chave `id_chamado`
# MAGIC O arquivo de cada dia traz chamados novos **e** atualizações de status de chamados antigos.

# COMMAND ----------

validos = chamados.filter(~F.col("reprovado"))
mediana_por_tipo = validos.groupBy("tipo_problema").agg(
    F.expr("percentile_approx(domicilios_afetados, 0.5)").alias("mediana_domicilios"))

lote = ultimo_por_chave(
    validos.join(mediana_por_tipo, "tipo_problema", "left").withColumn(
        "domicilios_afetados",
        F.when(F.col("domicilios_afetados") > 0, F.col("domicilios_afetados")).otherwise(F.col("mediana_domicilios"))),
    ["id_chamado"],
    [F.col("atualizado_em").desc_nulls_last(), F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select(
    "id_chamado", "tipo_problema", "status",
    F.col("bairro_oficial").alias("bairro"),
    F.trim("logradouro").alias("logradouro"),
    F.col("numero").cast("string").alias("numero"),
    "latitude", "longitude",
    F.col("domicilios_afetados").cast("int").alias("domicilios_afetados"),
    "canal",
    F.coalesce(F.trim("descricao"), F.lit("")).alias("descricao"),
    F.initcap(F.trim("cidadao_nome")).alias("nome"),
    "cpf",
    F.trim("cidadao_telefone").alias("telefone"),
    F.lower(F.trim("cidadao_email")).alias("email"),
    "aberto_em",
    F.coalesce("atualizado_em", "aberto_em").alias("atualizado_em"),
    "resolvido_em",
    "arquivo_origem",
    F.current_timestamp().alias("processado_em"),
)

garantir_tabela(f"{S}.prata_chamados", f"""
CREATE TABLE IF NOT EXISTS {S}.prata_chamados (
  id_chamado STRING NOT NULL COMMENT 'Chave do chamado (AK-AAAAMMDD-TT-NNN)',
  tipo_problema STRING, status STRING, bairro STRING, logradouro STRING, numero STRING,
  latitude DOUBLE, longitude DOUBLE, domicilios_afetados INT, canal STRING, descricao STRING,
  nome STRING COMMENT 'Dado pessoal (fictício): não vai para a Ouro',
  cpf STRING COMMENT 'Dado pessoal (fictício): vira hash com salt na Ouro',
  telefone STRING COMMENT 'Dado pessoal (fictício): mascarado na Ouro',
  email STRING COMMENT 'Dado pessoal (fictício): não vai para a Ouro',
  aberto_em TIMESTAMP, atualizado_em TIMESTAMP, resolvido_em TIMESTAMP,
  arquivo_origem STRING, processado_em TIMESTAMP
) COMMENT 'Prata: último estado de cada chamado, tratado e validado. Contém dados pessoais (fictícios): acesso restrito à equipe de dados.'""")

display(merge_por_chave(lote, "prata_chamados", ["id_chamado"], "atualizado_em"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resultados para o relatório

# COMMAND ----------

display(ultimo_resumo_qualidade("prata_chamados"))

# COMMAND ----------

display(spark.table(f"{S}.prata_chamados").groupBy("status").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histórico do MERGE (no lote 2 aparecem linhas **atualizadas**: são as mudanças de status)

# COMMAND ----------

display(historico_merge("prata_chamados"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.akona.prata_chamados;
