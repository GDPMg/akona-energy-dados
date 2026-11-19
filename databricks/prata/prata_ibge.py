# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · Prata · IBGE
# MAGIC
# MAGIC `bronze_ibge` (envelope com as respostas da API) → `prata_ibge_indicadores` (1 linha por indicador e ano).
# MAGIC Executado pela DAG `akona_ibge`.
# MAGIC
# MAGIC A resposta da API de agregados é aninhada: variável → resultados → séries → `{ano: valor}`. Na Bronze ela está
# MAGIC como texto JSON; aplicamos o schema com `from_json` e achatamos tudo. Como o IBGE é coletado todo dia, as dezenas
# MAGIC de arquivos viram só 6 linhas: é o MERGE (chave `cod_municipio + indicador_id + ano`) mostrando a idempotência.
# MAGIC
# MAGIC Para acrescentar outro agregado do IBGE no mesmo arquivo, basta incluir o nome do campo do envelope em `AGREGADOS`.

# COMMAND ----------

# MAGIC %run ../comum/funcoes

# COMMAND ----------

AGREGADOS = ["censo_populacao", "censo_domicilios"]     # campos do envelope com respostas da API de agregados
ESQUEMA_MUNICIPIO = "struct<id:string, nome:string, microrregiao:struct<mesorregiao:struct<UF:struct<sigla:string>>>>"
ESQUEMA_AGREGADO = ("array<struct<id:string, variavel:string, unidade:string, resultados:array<struct<"
                    "series:array<struct<localidade:struct<id:string, nome:string>, serie:map<string,string>>>>>>>")

bronze = spark.table(f"{S}.bronze_ibge").withColumn("municipio", F.from_json("municipio", ESQUEMA_MUNICIPIO))
for campo in AGREGADOS:
    bronze = bronze.withColumn(campo, F.from_json(campo, ESQUEMA_AGREGADO))


def explodir_agregado(coluna):
    return (
        bronze
        .select("arquivo_origem", "carregado_em",
                F.col("municipio.nome").alias("municipio"),
                F.col("municipio.microrregiao.mesorregiao.UF.sigla").alias("uf"),
                F.explode(coluna).alias("variavel"))
        .select("*", F.explode("variavel.resultados").alias("resultado"))
        .select("*", F.explode("resultado.series").alias("serie"))
        .select("arquivo_origem", "carregado_em", "municipio", "uf",
                F.col("serie.localidade.id").alias("cod_municipio"),
                F.col("variavel.id").alias("indicador_id"),
                F.col("variavel.variavel").alias("indicador"),
                F.col("variavel.unidade").alias("unidade"),
                F.explode("serie.serie").alias("ano", "valor_texto"))                 # mapa {ano: valor}
    )


indicadores = (
    reduce(lambda a, b: a.unionByName(b), [explodir_agregado(c) for c in AGREGADOS])
    .withColumn("valor", F.expr("try_cast(valor_texto AS DOUBLE)"))
)

# COMMAND ----------

REGRAS = [
    ("I1_valor_positivo", "valor numérico e maior que zero", F.coalesce(F.col("valor") <= 0, F.lit(True))),
]
ibge = aplicar_regras(indicadores, REGRAS)
display(registrar_qualidade("prata_ibge_indicadores", REGRAS, ibge))

lote = ultimo_por_chave(
    ibge.filter(~F.col("reprovado")), ["cod_municipio", "indicador_id", "ano"],
    [F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select("cod_municipio", "municipio", "uf", "indicador_id", "indicador", "unidade", "ano", "valor",
         "arquivo_origem", "carregado_em", F.current_timestamp().alias("processado_em"))

garantir_tabela(f"{S}.prata_ibge_indicadores", f"""
CREATE TABLE IF NOT EXISTS {S}.prata_ibge_indicadores (
  cod_municipio STRING NOT NULL, municipio STRING, uf STRING,
  indicador_id STRING NOT NULL, indicador STRING, unidade STRING, ano STRING NOT NULL, valor DOUBLE,
  arquivo_origem STRING, carregado_em TIMESTAMP, processado_em TIMESTAMP
) COMMENT 'Prata: indicadores do IBGE (Censo 2022) de Osasco, um registro por indicador e ano'""")

display(merge_por_chave(lote, "prata_ibge_indicadores", ["cod_municipio", "indicador_id", "ano"], "carregado_em"))

# COMMAND ----------

display(spark.table(f"{S}.prata_ibge_indicadores").select("indicador_id", "indicador", "unidade", "ano", "valor"))
