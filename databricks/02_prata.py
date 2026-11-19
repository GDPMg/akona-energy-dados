# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · 02 · Prata (tratamento, qualidade e MERGE)
# MAGIC
# MAGIC Transforma a Bronze (dado bruto, tudo texto) em dado **tratado e confiável**:
# MAGIC
# MAGIC 1. **Tipos corretos:** datas viram `TIMESTAMP`, coordenadas `DOUBLE` e domicílios `INT`.
# MAGIC 2. **Nulos e inválidos tratados:** texto normalizado (tipo, status e bairro), datas aceitas em 2 formatos, domicílios nulos recebem a mediana do tipo e canal nulo vira `nao_informado`.
# MAGIC 3. **Regras de qualidade com contagem:** quem viola uma regra é **barrado** (vai para `prata_chamados_rejeitados`) e o total barrado por regra fica em `controle_qualidade`.
# MAGIC 4. **MERGE pela chave:** `id_chamado` (chamados), `cod_municipio + data_hora` (clima) e `cod_municipio + indicador_id + ano` (IBGE). Rodar de novo não duplica nada.
# MAGIC
# MAGIC O notebook relê a Bronze inteira a cada execução e o MERGE garante a idempotência.
# MAGIC Usamos `try_cast` / `try_to_timestamp` porque o serverless roda com **ANSI ligado**: um `cast` comum quebraria o job ao encontrar um valor inválido.

# COMMAND ----------

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

for conf, valor in [("spark.sql.session.timeZone", "America/Sao_Paulo"),       # horários de Brasília
                    ("spark.sql.legacy.timeParserPolicy", "CORRECTED")]:     # data ilegível vira NULL
    try:
        spark.conf.set(conf, valor)
    except Exception as erro:  # o serverless pode bloquear alguma configuração; seguimos com o padrão
        print(f"Aviso: não foi possível definir {conf}: {erro}")

S = "workspace.akona"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.controle_qualidade (
  execucao_em TIMESTAMP, tabela STRING, regra STRING, descricao STRING,
  total_lido BIGINT, barrados BIGINT
) COMMENT 'Log das regras de qualidade da Prata: total barrado por regra (requisito 06)'
""")

TIPOS = ["fio_caido", "queda_energia", "poste_apagado", "lampada_piscando"]
STATUS = ["aberto", "em_andamento", "resolvido"]
BAIRROS_OSASCO = [
    "Centro", "Vila Yara", "Presidente Altino", "Bela Vista", "Jaguaribe", "Km 18",
    "Quitaúna", "Rochdale", "Novo Osasco", "Pestana", "Bonfim", "Vila Campesina",
    "Jardim das Flores", "Munhoz Júnior", "Santo Antônio", "Ayrosa",
]
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = -23.58, -23.47, -46.84, -46.73   # retângulo que contém Osasco


def chave(coluna):
    """Normaliza texto livre: minúsculas, sem acento e com '_' (' Queda de energia ' -> 'queda_energia')."""
    texto = F.translate(F.lower(F.trim(coluna)), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc")
    texto = F.regexp_replace(texto, "[^a-z0-9]+", "_")
    texto = F.regexp_replace(texto, "^_+|_+$", "")
    texto = F.regexp_replace(texto, "_(de|da|do)_", "_")
    return F.when(texto != "", texto)


def para_timestamp(nome):
    """Aceita 'yyyy-MM-dd HH:mm:ss' e 'dd/MM/yyyy HH:mm'; valor ilegível vira NULL."""
    return F.coalesce(
        F.expr(f"try_to_timestamp({nome}, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({nome}, 'dd/MM/yyyy HH:mm')"),
    )


def fora(coluna, minimo, maximo):
    """True quando o valor está fora da faixa (NULL também conta como fora)."""
    return F.coalesce(~F.col(coluna).between(minimo, maximo), F.lit(True))


def aplicar_regras(df, regras):
    """Cria uma coluna booleana por regra (True = viola a regra) e a coluna 'reprovado'."""
    for nome, _, condicao in regras:
        df = df.withColumn(nome, condicao)
    return df.withColumn("reprovado", reduce(lambda a, b: a | b, [F.col(n) for n, _, _ in regras]))


def registrar_qualidade(tabela, regras, df_com_regras):
    """Grava em controle_qualidade o total barrado por regra e devolve o resumo."""
    agregados = [F.count("*").alias("total_lido")] + [F.sum(F.col(n).cast("int")).alias(n) for n, _, _ in regras]
    contagem = df_com_regras.agg(*agregados).first()
    linhas = [(tabela, n, d, contagem["total_lido"], contagem[n] or 0) for n, d, _ in regras]
    resumo = (
        spark.createDataFrame(linhas, "tabela STRING, regra STRING, descricao STRING, total_lido BIGINT, barrados BIGINT")
        .withColumn("execucao_em", F.current_timestamp())
        .select("execucao_em", "tabela", "regra", "descricao", "total_lido", "barrados")
    )
    resumo.write.mode("append").saveAsTable(f"{S}.controle_qualidade")
    return resumo


def ultimo_por_chave(df, chaves, ordem):
    """Deduplica: mantém 1 linha por chave (a primeira segundo 'ordem'). O MERGE exige isso."""
    janela = Window.partitionBy(*chaves).orderBy(*ordem)
    return df.withColumn("_ordem", F.row_number().over(janela)).filter("_ordem = 1").drop("_ordem")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Chamados
# MAGIC ### 1.1 Tipos, normalização e tratamento de nulos

# COMMAND ----------

bronze_chamados = spark.table(f"{S}.bronze_chamados")
ref_bairros = (
    spark.createDataFrame([(b,) for b in BAIRROS_OSASCO], "bairro_oficial STRING")
    .withColumn("bairro_chave", chave(F.col("bairro_oficial")))
)

tratado = (
    bronze_chamados
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
# MAGIC ### 1.2 Regras de qualidade (total barrado por regra)
# MAGIC | Regra | Barra o registro quando |
# MAGIC |---|---|
# MAGIC | R1_obrigatorios | id, tipo ou data de abertura está nulo ou ilegível |
# MAGIC | R2_dominio | tipo fora dos 4 tipos aceitos ou status fora de aberto / em_andamento / resolvido |
# MAGIC | R3_localizacao | bairro não é de Osasco ou latitude/longitude fora do município (ex.: GPS 0,0) |
# MAGIC | R4_datas_coerentes | data de resolução anterior à data de abertura |

# COMMAND ----------

REGRAS_CHAMADOS = [
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

chamados = aplicar_regras(tratado, REGRAS_CHAMADOS)
display(registrar_qualidade("prata_chamados", REGRAS_CHAMADOS, chamados))

# Quarentena: registros barrados, com o motivo (sem dados pessoais)
rejeitados = (
    chamados.filter("reprovado")
    .withColumn("motivos", F.concat_ws(", ", *[F.when(F.col(n), F.lit(n)) for n, _, _ in REGRAS_CHAMADOS]))
    .select("id_chamado", "motivos", "tipo_bruto", "status_bruto", F.col("bairro").alias("bairro_bruto"),
            "latitude", "longitude", "aberto_em", "resolvido_em", "arquivo_origem")
)
rejeitados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.prata_chamados_rejeitados")
display(spark.table(f"{S}.prata_chamados_rejeitados").groupBy("motivos").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Deduplicação e MERGE pela chave `id_chamado`
# MAGIC O arquivo de cada dia traz chamados novos **e** atualizações de status de chamados antigos.
# MAGIC Por isso o MERGE só atualiza quando o registro que chegou é **mais recente** (`atualizado_em` maior).

# COMMAND ----------

validos = chamados.filter(~F.col("reprovado"))
mediana_por_tipo = validos.groupBy("tipo_problema").agg(
    F.expr("percentile_approx(domicilios_afetados, 0.5)").alias("mediana_domicilios"))

lote_chamados = ultimo_por_chave(
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

spark.sql(f"""
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
) COMMENT 'Prata: último estado de cada chamado, tratado e validado. Contém dados pessoais (fictícios): acesso restrito à equipe de dados.'
""")

lote_chamados.createOrReplaceTempView("lote_prata_chamados")
display(spark.sql(f"""
MERGE INTO {S}.prata_chamados AS alvo
USING lote_prata_chamados AS novo
ON alvo.id_chamado = novo.id_chamado
WHEN MATCHED AND novo.atualizado_em > alvo.atualizado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clima horário (Open-Meteo)
# MAGIC Na Bronze o objeto `hourly` está guardado como **texto JSON** (dado bruto). Aqui aplicamos o schema com `from_json`.
# MAGIC A API devolve **listas paralelas** (`time[i]`, `precipitation[i]`, ...). O `posexplode` gera uma linha por hora
# MAGIC com a posição `i`, e usamos a mesma posição para pegar o valor de cada lista.

# COMMAND ----------

ESQUEMA_HOURLY = ("struct<time:array<string>, precipitation:array<string>, wind_speed_10m:array<string>, "
                  "wind_gusts_10m:array<string>, temperature_2m:array<string>, weather_code:array<string>>")


def valor_na_hora(campo, tipo="DOUBLE"):
    return F.expr(f"try_cast(try_element_at(hourly.{campo}, pos + 1) AS {tipo})")


horas = (
    spark.table(f"{S}.bronze_clima")
    .withColumn("hourly", F.from_json("hourly", ESQUEMA_HOURLY))      # texto JSON -> estrutura
    .select("arquivo_origem", "carregado_em", "hourly",
            F.posexplode("hourly.time").alias("pos", "hora_texto"))
    .select(
        F.lit("3534401").alias("cod_municipio"),
        F.expr("try_to_timestamp(replace(hora_texto, 'T', ' '), 'yyyy-MM-dd HH:mm')").alias("data_hora"),
        valor_na_hora("precipitation").alias("precipitacao_mm"),
        valor_na_hora("wind_speed_10m").alias("vento_kmh"),
        valor_na_hora("wind_gusts_10m").alias("rajada_kmh"),
        valor_na_hora("temperature_2m").alias("temperatura_c"),
        valor_na_hora("weather_code", "INT").alias("codigo_tempo"),
        "arquivo_origem", "carregado_em",
    )
)

REGRAS_CLIMA = [
    ("C1_hora_valida", "data/hora legível", F.col("data_hora").isNull()),
    ("C2_chuva_plausivel", "chuva entre 0 e 200 mm/h", fora("precipitacao_mm", 0, 200)),
    ("C3_rajada_plausivel", "rajada entre 0 e 250 km/h", fora("rajada_kmh", 0, 250)),
]
clima = aplicar_regras(horas, REGRAS_CLIMA)
display(registrar_qualidade("prata_clima_horario", REGRAS_CLIMA, clima))

lote_clima = ultimo_por_chave(
    clima.filter(~F.col("reprovado")), ["cod_municipio", "data_hora"],
    [F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select("cod_municipio", "data_hora", "precipitacao_mm", "vento_kmh", "rajada_kmh", "temperatura_c",
         "codigo_tempo", "arquivo_origem", "carregado_em", F.current_timestamp().alias("processado_em"))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.prata_clima_horario (
  cod_municipio STRING NOT NULL, data_hora TIMESTAMP NOT NULL,
  precipitacao_mm DOUBLE, vento_kmh DOUBLE, rajada_kmh DOUBLE, temperatura_c DOUBLE, codigo_tempo INT,
  arquivo_origem STRING, carregado_em TIMESTAMP, processado_em TIMESTAMP
) COMMENT 'Prata: clima horário de Osasco (Open-Meteo), uma linha por hora'
""")

lote_clima.createOrReplaceTempView("lote_prata_clima")
display(spark.sql(f"""
MERGE INTO {S}.prata_clima_horario AS alvo
USING lote_prata_clima AS novo
ON alvo.cod_municipio = novo.cod_municipio AND alvo.data_hora = novo.data_hora
WHEN MATCHED AND novo.carregado_em > alvo.carregado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Indicadores do IBGE (Censo 2022)
# MAGIC A resposta da API de agregados é aninhada: variável → resultados → séries → `{ano: valor}`. Na Bronze ela está
# MAGIC guardada como texto JSON; aplicamos o schema com `from_json` e achatamos tudo em uma linha por indicador e ano.
# MAGIC Como o IBGE é coletado todo dia, as dezenas de arquivos viram só 6 linhas na Prata: é o MERGE mostrando a idempotência.

# COMMAND ----------

ESQUEMA_MUNICIPIO = "struct<id:string, nome:string, microrregiao:struct<mesorregiao:struct<UF:struct<sigla:string>>>>"
ESQUEMA_AGREGADO = ("array<struct<id:string, variavel:string, unidade:string, resultados:array<struct<"
                    "series:array<struct<localidade:struct<id:string, nome:string>, serie:map<string,string>>>>>>>")

bronze_ibge = (
    spark.table(f"{S}.bronze_ibge")
    .withColumn("municipio", F.from_json("municipio", ESQUEMA_MUNICIPIO))
    .withColumn("censo_populacao", F.from_json("censo_populacao", ESQUEMA_AGREGADO))
    .withColumn("censo_domicilios", F.from_json("censo_domicilios", ESQUEMA_AGREGADO))
)


def explodir_agregado(coluna):
    return (
        bronze_ibge
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
    explodir_agregado("censo_populacao").unionByName(explodir_agregado("censo_domicilios"))
    .withColumn("valor", F.expr("try_cast(valor_texto AS DOUBLE)"))
)

REGRAS_IBGE = [
    ("I1_valor_positivo", "valor numérico e maior que zero", F.coalesce(F.col("valor") <= 0, F.lit(True))),
]
ibge = aplicar_regras(indicadores, REGRAS_IBGE)
display(registrar_qualidade("prata_ibge_indicadores", REGRAS_IBGE, ibge))

lote_ibge = ultimo_por_chave(
    ibge.filter(~F.col("reprovado")), ["cod_municipio", "indicador_id", "ano"],
    [F.col("carregado_em").desc(), F.col("arquivo_origem").desc()],
).select("cod_municipio", "municipio", "uf", "indicador_id", "indicador", "unidade", "ano", "valor",
         "arquivo_origem", "carregado_em", F.current_timestamp().alias("processado_em"))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.prata_ibge_indicadores (
  cod_municipio STRING NOT NULL, municipio STRING, uf STRING,
  indicador_id STRING NOT NULL, indicador STRING, unidade STRING, ano STRING NOT NULL, valor DOUBLE,
  arquivo_origem STRING, carregado_em TIMESTAMP, processado_em TIMESTAMP
) COMMENT 'Prata: indicadores do IBGE (Censo 2022) de Osasco, um registro por indicador e ano'
""")

lote_ibge.createOrReplaceTempView("lote_prata_ibge")
display(spark.sql(f"""
MERGE INTO {S}.prata_ibge_indicadores AS alvo
USING lote_prata_ibge AS novo
ON alvo.cod_municipio = novo.cod_municipio AND alvo.indicador_id = novo.indicador_id AND alvo.ano = novo.ano
WHEN MATCHED AND novo.carregado_em > alvo.carregado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resultados para o relatório
# MAGIC ### 4.1 Total barrado por regra (última execução de cada tabela)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.tabela, c.regra, c.descricao, c.total_lido, c.barrados
# MAGIC FROM workspace.akona.controle_qualidade c
# MAGIC JOIN (SELECT tabela, MAX(execucao_em) AS ultima FROM workspace.akona.controle_qualidade GROUP BY tabela) u
# MAGIC   ON c.tabela = u.tabela AND c.execucao_em = u.ultima
# MAGIC ORDER BY c.tabela, c.regra;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Linhas por tabela e status dos chamados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'prata_chamados' AS tabela, COUNT(*) AS linhas FROM workspace.akona.prata_chamados
# MAGIC UNION ALL SELECT 'prata_chamados_rejeitados', COUNT(*) FROM workspace.akona.prata_chamados_rejeitados
# MAGIC UNION ALL SELECT 'prata_clima_horario', COUNT(*) FROM workspace.akona.prata_clima_horario
# MAGIC UNION ALL SELECT 'prata_ibge_indicadores', COUNT(*) FROM workspace.akona.prata_ibge_indicadores;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, COUNT(*) AS chamados FROM workspace.akona.prata_chamados GROUP BY status ORDER BY chamados DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Tipos corretos na Prata

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.akona.prata_chamados;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Histórico do MERGE (no lote 2 aparecem linhas **atualizadas**: são as mudanças de status)

# COMMAND ----------

for tabela in ["prata_chamados", "prata_clima_horario", "prata_ibge_indicadores"]:
    display(
        spark.sql(f"DESCRIBE HISTORY {S}.{tabela}")
        .filter("operation = 'MERGE'")
        .select(F.lit(tabela).alias("tabela"), "version", "timestamp",
                F.col("operationMetrics").getItem("numSourceRows").alias("linhas_na_origem"),
                F.col("operationMetrics").getItem("numTargetRowsInserted").alias("inseridas"),
                F.col("operationMetrics").getItem("numTargetRowsUpdated").alias("atualizadas"))
        .orderBy("version")
    )
