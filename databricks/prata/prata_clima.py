# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · Prata · clima
# MAGIC
# MAGIC `bronze_clima` (1 linha por arquivo/dia) → `prata_clima_horario` (1 linha por hora). Executado pela DAG `akona_clima`.
# MAGIC
# MAGIC Na Bronze o objeto `hourly` está guardado como **texto JSON** (dado bruto); aqui aplicamos o schema com `from_json`.
# MAGIC A API devolve **listas paralelas** (`time[i]`, `precipitation[i]`, ...): o `posexplode` gera uma linha por hora
# MAGIC com a posição `i`, e a mesma posição pega o valor de cada lista. MERGE pela chave `cod_municipio + data_hora`.

# COMMAND ----------

# MAGIC %run ../comum/funcoes

# COMMAND ----------

COD_MUNICIPIO = "3534401"   # Osasco (IBGE): o arquivo do Open-Meteo traz só as coordenadas
ESQUEMA_HOURLY = ("struct<time:array<string>, precipitation:array<string>, wind_speed_10m:array<string>, "
                  "wind_gusts_10m:array<string>, temperature_2m:array<string>, weather_code:array<string>>")


def valor_na_hora(campo, tipo="DOUBLE"):
    return F.expr(f"try_cast(try_element_at(hourly.{campo}, pos + 1) AS {tipo})")


horas = (
    spark.table(f"{S}.bronze_clima")
    .withColumn("hourly", F.from_json("hourly", ESQUEMA_HOURLY))
    .select("arquivo_origem", "carregado_em", "hourly",
            F.posexplode("hourly.time").alias("pos", "hora_texto"))
    .select(
        F.lit(COD_MUNICIPIO).alias("cod_municipio"),
        F.expr("try_to_timestamp(replace(hora_texto, 'T', ' '), 'yyyy-MM-dd HH:mm')").alias("data_hora"),
        valor_na_hora("precipitation").alias("precipitacao_mm"),
        valor_na_hora("wind_speed_10m").alias("vento_kmh"),
        valor_na_hora("wind_gusts_10m").alias("rajada_kmh"),
        valor_na_hora("temperature_2m").alias("temperatura_c"),
        valor_na_hora("weather_code", "INT").alias("codigo_tempo"),
        "arquivo_origem", "carregado_em",
    )
)

# COMMAND ----------

REGRAS = [
    ("C1_hora_valida", "data/hora legível", F.col("data_hora").isNull()),
    ("C2_chuva_plausivel", "chuva entre 0 e 200 mm/h", fora("precipitacao_mm", 0, 200)),
    ("C3_rajada_plausivel", "rajada entre 0 e 250 km/h", fora("rajada_kmh", 0, 250)),
]
clima = aplicar_regras(horas, REGRAS)
display(registrar_qualidade("prata_clima_horario", REGRAS, clima))

lote = ultimo_por_chave(
    clima.filter(~F.col("reprovado")), ["cod_municipio", "data_hora"],
    [F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select("cod_municipio", "data_hora", "precipitacao_mm", "vento_kmh", "rajada_kmh", "temperatura_c",
         "codigo_tempo", "arquivo_origem", "carregado_em", F.current_timestamp().alias("processado_em"))

garantir_tabela(f"{S}.prata_clima_horario", f"""
CREATE TABLE IF NOT EXISTS {S}.prata_clima_horario (
  cod_municipio STRING NOT NULL, data_hora TIMESTAMP NOT NULL,
  precipitacao_mm DOUBLE, vento_kmh DOUBLE, rajada_kmh DOUBLE, temperatura_c DOUBLE, codigo_tempo INT,
  arquivo_origem STRING, carregado_em TIMESTAMP, processado_em TIMESTAMP
) COMMENT 'Prata: clima horário de Osasco (Open-Meteo), uma linha por hora'""")

display(merge_por_chave(lote, "prata_clima_horario", ["cod_municipio", "data_hora"], "carregado_em"))

# COMMAND ----------

display(historico_merge("prata_clima_horario"))
