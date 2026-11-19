# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · funções comuns (Bronze e Prata)
# MAGIC
# MAGIC Incluído no início dos notebooks com `%run ../comum/funcoes`. Concentra o que toda fonte usa:
# MAGIC nomes do catálogo, tabelas de controle, normalização de texto e datas, regras de qualidade e MERGE.
# MAGIC Uma fonte nova não precisa reescrever nada disto.

# COMMAND ----------

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOGO, SCHEMA, VOLUME = "workspace", "akona", "chegada"
S = f"{CATALOGO}.{SCHEMA}"
BASE_VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"

# Bairros de Osasco atendidos
BAIRROS_OSASCO = [
    "Centro", "Vila Yara", "Presidente Altino", "Bela Vista", "Jaguaribe", "Km 18",
    "Quitaúna", "Rochdale", "Novo Osasco", "Pestana", "Bonfim", "Vila Campesina",
    "Jardim das Flores", "Munhoz Júnior", "Santo Antônio", "Ayrosa",
]

for _conf, _valor in [("spark.sql.session.timeZone", "America/Sao_Paulo"),
                      ("spark.sql.legacy.timeParserPolicy", "CORRECTED")]:   # data ilegível vira NULL
    try:
        spark.conf.set(_conf, _valor)
    except Exception as _erro:  # o serverless pode bloquear alguma configuração; seguimos com o padrão
        print(f"Aviso: não foi possível definir {_conf}: {_erro}")


def garantir_tabela(nome: str, ddl: str) -> None:
    """CREATE TABLE IF NOT EXISTS seguro para DAGs rodando em paralelo (se outra criou antes, tudo bem)."""
    try:
        spark.sql(ddl)
    except Exception:
        if not spark.catalog.tableExists(nome):
            raise


garantir_tabela(f"{S}.controle_cargas_bronze", f"""
CREATE TABLE IF NOT EXISTS {S}.controle_cargas_bronze (
  execucao_em TIMESTAMP, lote STRING, fonte STRING, tabela STRING,
  linhas_antes BIGINT, linhas_depois BIGINT, linhas_novas BIGINT,
  arquivos_antes BIGINT, arquivos_depois BIGINT
) COMMENT 'Log de cada execução da Bronze: contagens antes e depois (requisito 05)'""")

garantir_tabela(f"{S}.controle_qualidade", f"""
CREATE TABLE IF NOT EXISTS {S}.controle_qualidade (
  execucao_em TIMESTAMP, tabela STRING, regra STRING, descricao STRING,
  total_lido BIGINT, barrados BIGINT
) COMMENT 'Log das regras de qualidade da Prata: total barrado por regra (requisito 06)'""")

# COMMAND ----------


def chave(coluna):
    """Normaliza texto livre: minúsculas, sem acento e com '_' (' Queda de energia ' -> 'queda_energia')."""
    texto = F.translate(F.lower(F.trim(coluna)), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc")
    texto = F.regexp_replace(texto, "[^a-z0-9]+", "_")
    texto = F.regexp_replace(texto, "^_+|_+$", "")
    texto = F.regexp_replace(texto, "_(de|da|do)_", "_")
    return F.when(texto != "", texto)


def para_timestamp(nome: str, formatos=("yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm")):
    """Tenta cada formato; valor ilegível vira NULL (try_to_timestamp não quebra com ANSI ligado)."""
    return F.coalesce(*[F.expr(f"try_to_timestamp({nome}, '{formato}')") for formato in formatos])


def referencia_bairros():
    """DataFrame (bairro_oficial, bairro_chave) para normalizar o bairro com um join pela chave."""
    return (spark.createDataFrame([(b,) for b in BAIRROS_OSASCO], "bairro_oficial STRING")
            .withColumn("bairro_chave", chave(F.col("bairro_oficial"))))


def fora(coluna: str, minimo, maximo):
    """True quando o valor está fora da faixa (NULL também conta como fora)."""
    return F.coalesce(~F.col(coluna).between(minimo, maximo), F.lit(True))


def aplicar_regras(df, regras):
    """regras = [(nome, descrição, condição que VIOLA a regra)]. Cria uma coluna por regra e a coluna 'reprovado'."""
    for nome, _, condicao in regras:
        df = df.withColumn(nome, condicao)
    return df.withColumn("reprovado", reduce(lambda a, b: a | b, [F.col(n) for n, _, _ in regras]))


def registrar_qualidade(tabela: str, regras, df_com_regras):
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


def merge_por_chave(df, tabela: str, chaves, coluna_mais_recente: str):
    """MERGE INTO {S}.<tabela>: insere chaves novas e só atualiza quando o registro novo é mais recente."""
    visao = f"lote_{tabela}"
    df.createOrReplaceTempView(visao)
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    return spark.sql(f"""
        MERGE INTO {S}.{tabela} AS alvo
        USING {visao} AS novo
        ON {condicao}
        WHEN MATCHED AND novo.{coluna_mais_recente} > alvo.{coluna_mais_recente} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def historico_merge(tabela: str):
    """Histórico dos MERGE da tabela: linhas inseridas e atualizadas em cada execução (print do relatório)."""
    return (
        spark.sql(f"DESCRIBE HISTORY {S}.{tabela}")
        .filter("operation = 'MERGE'")
        .select("version", "timestamp",
                F.col("operationMetrics").getItem("numSourceRows").alias("linhas_na_origem"),
                F.col("operationMetrics").getItem("numTargetRowsInserted").alias("inseridas"),
                F.col("operationMetrics").getItem("numTargetRowsUpdated").alias("atualizadas"))
        .orderBy("version")
    )


def ultimo_resumo_qualidade(tabela: str):
    """Total barrado por regra na última execução da tabela."""
    return spark.sql(f"""
        SELECT regra, descricao, total_lido, barrados FROM {S}.controle_qualidade
        WHERE tabela = '{tabela}'
          AND execucao_em = (SELECT MAX(execucao_em) FROM {S}.controle_qualidade WHERE tabela = '{tabela}')
        ORDER BY regra""")
