# Databricks notebook source
# MAGIC %md
# MAGIC # Akona Energy · 03 · Ouro (modelo dimensional + LGPD)
# MAGIC
# MAGIC Modelo estrela pronto para as perguntas do MVP. É reconstruído inteiro a cada execução (sobrescrita), então rodar de novo é seguro.
# MAGIC
# MAGIC | Tabela | Tipo | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | `ouro_fato_chamados` | **fato** | 1 linha por chamado (último estado): tempos, impacto, SLA e prioridade |
# MAGIC | `ouro_dim_tempo` | dimensão | calendário (dia, mês, dia da semana) |
# MAGIC | `ouro_dim_clima` | dimensão | clima do dia em Osasco (chuva, rajada, condição seco/chuva/temporal) |
# MAGIC | `ouro_dim_bairro` | dimensão | bairro + indicadores do IBGE (população, média de moradores por domicílio) |
# MAGIC | `ouro_dim_tipo_problema` | dimensão | categoria, responsável, gravidade, peso na fila e SLA de referência |
# MAGIC | `ouro_dim_cidadao` | dimensão | cidadão **pseudonimizado** (hash do CPF + nome e telefone mascarados) |
# MAGIC | `ouro_mvp_painel_chamados` | **contrato do MVP** | única tabela lida pela tela do MVP (sem dado pessoal em claro) |
# MAGIC
# MAGIC **LGPD:** o CPF vira `sha2(salt + cpf, 256)`. O *salt* fica no **Databricks Secrets** (escopo `akona`, chave `salt_lgpd`):
# MAGIC foi gerado aleatoriamente pelo Airflow, ninguém vê o valor e ele **nunca** vai para o repositório.
# MAGIC Sem salt, daria para descobrir o CPF testando os ~10⁹ CPFs possíveis. Nome e telefone ficam mascarados, e-mail e número da casa
# MAGIC não entram na Ouro e as coordenadas são arredondadas para 3 casas (~100 m).

# COMMAND ----------

try:
    SALT = dbutils.secrets.get("akona", "salt_lgpd")       # criado pela tarefa preparar_databricks do Airflow
except Exception:
    dbutils.widgets.text("salt_lgpd", "", "Salt LGPD (só se o segredo não existir)")
    SALT = dbutils.widgets.get("salt_lgpd").strip()
if len(SALT) < 12:
    raise ValueError("Salt LGPD ausente: rode o DAG akona_lakehouse (ele cria o segredo akona/salt_lgpd) "
                     "ou preencha o widget 'salt_lgpd' com um segredo de 12+ caracteres.")

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
S = "workspace.akona"


def criar_tabela(nome, consulta, comentario, **parametros):
    """Executa a consulta (com parâmetros, se houver), sobrescreve a tabela da Ouro e grava o comentário."""
    df = spark.sql(consulta.format(S=S), args=parametros or None)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.{nome}")
    spark.sql(f"COMMENT ON TABLE {S}.{nome} IS '{comentario}'")
    print(f"{nome}: {spark.table(f'{S}.{nome}').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

# Tipo de problema: referência definida pela equipe (peso na fila e SLA de referência do projeto)
criar_tabela("ouro_dim_tipo_problema", """
SELECT * FROM VALUES
  (1, 'fio_caido',        'Fio caído',        'Rede elétrica',      'Distribuidora de energia', 4, 100,   8),
  (2, 'queda_energia',    'Queda de energia', 'Rede elétrica',      'Distribuidora de energia', 3,  80,  24),
  (3, 'poste_apagado',    'Poste apagado',    'Iluminação pública', 'Prefeitura de Osasco',     2,  40, 120),
  (4, 'lampada_piscando', 'Lâmpada piscando', 'Iluminação pública', 'Prefeitura de Osasco',     1,  20, 168)
AS t(tipo_key, tipo_problema, tipo_descricao, categoria, responsavel, gravidade, peso_prioridade, sla_horas)
""", "Dimensão tipo de problema: categoria, responsável, gravidade, peso na fila de prioridade e SLA de referência")

# Bairro: bairros que aparecem nos chamados + indicadores do município (IBGE, Censo 2022)
criar_tabela("ouro_dim_bairro", """
WITH ibge AS (
  SELECT MAX(cod_municipio) AS cod_ibge_municipio,
         MAX(municipio) AS municipio,
         MAX(uf) AS uf,
         MAX(CASE WHEN indicador_id = '93'   THEN valor END) AS populacao_municipio,
         MAX(CASE WHEN indicador_id = '381'  THEN valor END) AS domicilios_municipio,
         MAX(CASE WHEN indicador_id = '5930' THEN valor END) AS media_moradores_domicilio,
         MAX(CASE WHEN indicador_id = '614'  THEN valor END) AS densidade_hab_km2
  FROM {S}.prata_ibge_indicadores
  WHERE ano = '2022'
)
SELECT xxhash64(b.bairro) AS bairro_key,
       b.bairro, i.municipio, i.uf, i.cod_ibge_municipio,
       CAST(i.populacao_municipio AS BIGINT) AS populacao_municipio,
       CAST(i.domicilios_municipio AS BIGINT) AS domicilios_municipio,
       i.media_moradores_domicilio,
       i.densidade_hab_km2
FROM (SELECT DISTINCT bairro FROM {S}.prata_chamados) b
CROSS JOIN ibge i
""", "Dimensão bairro de Osasco, enriquecida com indicadores do IBGE (Censo 2022)")

# Tempo: um registro por dia entre o primeiro e o último dia com dados
criar_tabela("ouro_dim_tempo", """
WITH limites AS (
  SELECT LEAST(c.inicio, k.inicio) AS inicio, GREATEST(c.fim, k.fim) AS fim
  FROM (SELECT MIN(DATE(aberto_em)) AS inicio, MAX(DATE(atualizado_em)) AS fim FROM {S}.prata_chamados) c
  CROSS JOIN (SELECT MIN(DATE(data_hora)) AS inicio, MAX(DATE(data_hora)) AS fim FROM {S}.prata_clima_horario) k
),
dias AS (SELECT explode(sequence(inicio, fim, INTERVAL 1 DAY)) AS data FROM limites)
SELECT CAST(date_format(data, 'yyyyMMdd') AS INT) AS data_key,
       data,
       YEAR(data) AS ano,
       MONTH(data) AS mes,
       DAY(data) AS dia,
       CASE MONTH(data) WHEN 1 THEN 'janeiro' WHEN 2 THEN 'fevereiro' WHEN 3 THEN 'março' WHEN 4 THEN 'abril'
                        WHEN 5 THEN 'maio' WHEN 6 THEN 'junho' WHEN 7 THEN 'julho' WHEN 8 THEN 'agosto'
                        WHEN 9 THEN 'setembro' WHEN 10 THEN 'outubro' WHEN 11 THEN 'novembro' ELSE 'dezembro' END AS nome_mes,
       DAYOFWEEK(data) AS dia_semana_num,
       CASE DAYOFWEEK(data) WHEN 1 THEN 'domingo' WHEN 2 THEN 'segunda-feira' WHEN 3 THEN 'terça-feira'
                            WHEN 4 THEN 'quarta-feira' WHEN 5 THEN 'quinta-feira' WHEN 6 THEN 'sexta-feira'
                            ELSE 'sábado' END AS dia_semana,
       DAYOFWEEK(data) IN (1, 7) AS fim_de_semana
FROM dias
""", "Dimensão calendário (um registro por dia)")

# Clima: agrega as 24 horas de cada dia e classifica a condição do dia
criar_tabela("ouro_dim_clima", """
SELECT CAST(date_format(DATE(data_hora), 'yyyyMMdd') AS INT) AS data_key,
       DATE(data_hora) AS data,
       ROUND(SUM(precipitacao_mm), 1) AS precipitacao_mm,
       ROUND(MAX(rajada_kmh), 1) AS rajada_max_kmh,
       ROUND(MAX(vento_kmh), 1) AS vento_max_kmh,
       ROUND(AVG(temperatura_c), 1) AS temperatura_media_c,
       MAX(codigo_tempo) >= 95 AS teve_trovoada,
       CASE WHEN SUM(precipitacao_mm) >= 15 OR MAX(rajada_kmh) >= 50 THEN 'temporal'
            WHEN SUM(precipitacao_mm) >= 1 THEN 'chuva'
            ELSE 'seco' END AS condicao_clima,
       COUNT(*) AS horas_com_dado
FROM {S}.prata_clima_horario
GROUP BY DATE(data_hora)
""", "Dimensão clima diário de Osasco (Open-Meteo). Temporal = chuva de 15 mm ou mais no dia ou rajada de 50 km/h ou mais")

# Cidadão: pseudonimizado (LGPD). Só o hash do CPF identifica; nome e telefone saem mascarados.
criar_tabela("ouro_dim_cidadao", """
WITH ultimo AS (
  SELECT cpf,
         max_by(nome, atualizado_em) AS nome,
         max_by(telefone, atualizado_em) AS telefone,
         COUNT(*) AS total_chamados
  FROM {S}.prata_chamados
  WHERE cpf IS NOT NULL
  GROUP BY cpf
)
SELECT sha2(concat(:salt, cpf), 256) AS cidadao_key,
       array_join(transform(split(nome, ' '), p -> concat(left(p, 1), repeat('*', length(p) - 1))), ' ') AS nome_mascarado,
       concat('(', substr(regexp_replace(telefone, '[^0-9]', ''), 1, 2), ') *****-',
              right(regexp_replace(telefone, '[^0-9]', ''), 4)) AS telefone_mascarado,
       total_chamados
FROM ultimo
""", "Dimensão cidadão PSEUDONIMIZADA (LGPD): hash do CPF com salt secreto, nome e telefone mascarados", salt=SALT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato
# MAGIC - `tempo_resolucao_horas`: só para resolvidos. `horas_em_aberto`: para não resolvidos, até a **data de referência** (o evento mais recente da base).
# MAGIC - `moradores_afetados_estimados` = domicílios afetados × média de moradores por domicílio em Osasco (IBGE, Censo 2022).
# MAGIC - `prioridade_score` (só não resolvidos) = **peso do tipo** (fio 100, queda 80, poste 40, lâmpada 20)
# MAGIC   + até **30** pelo tempo de espera (teto de 7 dias) + **20** se o SLA venceu + até **20** pelo impacto (teto de 2.000 moradores).

# COMMAND ----------

criar_tabela("ouro_fato_chamados", """
WITH referencia AS (
  SELECT MAX(atualizado_em) AS data_referencia FROM {S}.prata_chamados
),
base AS (
  SELECT c.id_chamado,
         CAST(date_format(c.aberto_em, 'yyyyMMdd') AS INT) AS data_key,
         b.bairro_key,
         t.tipo_key,
         sha2(concat(:salt, c.cpf), 256) AS cidadao_key,
         c.status, c.canal, c.aberto_em, c.atualizado_em, c.resolvido_em,
         CASE WHEN c.status = 'resolvido'
              THEN ROUND(CAST(timestampdiff(MINUTE, c.aberto_em, c.resolvido_em) AS DOUBLE) / 60, 1) END AS tempo_resolucao_horas,
         CASE WHEN c.status <> 'resolvido'
              THEN ROUND(CAST(timestampdiff(MINUTE, c.aberto_em, r.data_referencia) AS DOUBLE) / 60, 1) END AS horas_em_aberto,
         c.domicilios_afetados,
         CAST(ROUND(c.domicilios_afetados * b.media_moradores_domicilio) AS INT) AS moradores_afetados_estimados,
         t.sla_horas,
         t.peso_prioridade,
         c.logradouro,
         ROUND(c.latitude, 3) AS latitude_aprox,
         ROUND(c.longitude, 3) AS longitude_aprox
  FROM {S}.prata_chamados c
  CROSS JOIN referencia r
  JOIN {S}.ouro_dim_bairro b ON b.bairro = c.bairro
  JOIN {S}.ouro_dim_tipo_problema t ON t.tipo_problema = c.tipo_problema
)
SELECT id_chamado, data_key, bairro_key, tipo_key, cidadao_key,
       status, canal, aberto_em, atualizado_em, resolvido_em,
       tempo_resolucao_horas, horas_em_aberto,
       COALESCE(tempo_resolucao_horas, horas_em_aberto) > sla_horas AS sla_vencido,
       domicilios_afetados, moradores_afetados_estimados,
       CASE WHEN status <> 'resolvido' THEN ROUND(
              peso_prioridade
            + LEAST(horas_em_aberto, 168) / 168 * 30
            + CASE WHEN horas_em_aberto > sla_horas THEN 20 ELSE 0 END
            + LEAST(moradores_afetados_estimados, 2000) / 2000 * 20, 1) END AS prioridade_score,
       logradouro, latitude_aprox, longitude_aprox
FROM base
""", "Fato: um registro por chamado (último estado), com tempos, impacto, SLA e prioridade. Sem dados pessoais em claro.",
   salt=SALT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrato com o produto: a tabela que o MVP consome
# MAGIC `ouro_mvp_painel_chamados` é a **única** tabela lida pela tela do MVP (painel AI/BI). Já vem com os rótulos
# MAGIC prontos para exibição, a posição na fila de prioridade e só dados pessoais **mascarados**.

# COMMAND ----------

criar_tabela("ouro_mvp_painel_chamados", """
SELECT f.id_chamado,
       tm.data AS data_abertura,
       f.aberto_em,
       f.atualizado_em,
       f.status,
       t.tipo_descricao AS tipo_problema,
       t.categoria,
       t.responsavel,
       b.bairro,
       f.logradouro,
       f.horas_em_aberto,
       f.tempo_resolucao_horas,
       t.sla_horas,
       f.sla_vencido,
       f.prioridade_score,
       CASE WHEN f.status <> 'resolvido'
            THEN ROW_NUMBER() OVER (PARTITION BY f.status = 'resolvido'
                                    ORDER BY f.prioridade_score DESC, f.aberto_em) END AS posicao_fila,
       f.domicilios_afetados,
       f.moradores_afetados_estimados,
       ci.nome_mascarado AS solicitante,
       ci.telefone_mascarado AS contato,
       cl.condicao_clima,
       cl.precipitacao_mm AS chuva_no_dia_mm,
       cl.rajada_max_kmh AS rajada_no_dia_kmh,
       f.latitude_aprox,
       f.longitude_aprox
FROM {S}.ouro_fato_chamados f
JOIN {S}.ouro_dim_tempo tm ON tm.data_key = f.data_key
JOIN {S}.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
JOIN {S}.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
LEFT JOIN {S}.ouro_dim_cidadao ci ON ci.cidadao_key = f.cidadao_key
LEFT JOIN {S}.ouro_dim_clima cl ON cl.data_key = f.data_key
""", "CONTRATO DE DADOS DO MVP Akona Energy: única tabela lida pelo painel. Atualização diária. Sem CPF, nome, telefone ou e-mail em claro.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferências para o relatório
# MAGIC ### Linhas por tabela da Ouro

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'ouro_fato_chamados' AS tabela, COUNT(*) AS linhas FROM workspace.akona.ouro_fato_chamados
# MAGIC UNION ALL SELECT 'ouro_dim_tempo', COUNT(*) FROM workspace.akona.ouro_dim_tempo
# MAGIC UNION ALL SELECT 'ouro_dim_clima', COUNT(*) FROM workspace.akona.ouro_dim_clima
# MAGIC UNION ALL SELECT 'ouro_dim_bairro', COUNT(*) FROM workspace.akona.ouro_dim_bairro
# MAGIC UNION ALL SELECT 'ouro_dim_tipo_problema', COUNT(*) FROM workspace.akona.ouro_dim_tipo_problema
# MAGIC UNION ALL SELECT 'ouro_dim_cidadao', COUNT(*) FROM workspace.akona.ouro_dim_cidadao
# MAGIC UNION ALL SELECT 'ouro_mvp_painel_chamados', COUNT(*) FROM workspace.akona.ouro_mvp_painel_chamados;

# COMMAND ----------

# MAGIC %md
# MAGIC ### LGPD: antes (Prata, acesso restrito) × depois (Ouro). Dados 100% fictícios.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.id_chamado,
# MAGIC        p.nome AS nome_prata, c.nome_mascarado AS nome_ouro,
# MAGIC        p.cpf AS cpf_prata, f.cidadao_key AS cpf_hash_ouro,
# MAGIC        p.telefone AS telefone_prata, c.telefone_mascarado AS telefone_ouro,
# MAGIC        concat(p.logradouro, ', ', p.numero) AS endereco_prata, f.logradouro AS endereco_ouro
# MAGIC FROM workspace.akona.prata_chamados p
# MAGIC JOIN workspace.akona.ouro_fato_chamados f USING (id_chamado)
# MAGIC JOIN workspace.akona.ouro_dim_cidadao c ON c.cidadao_key = f.cidadao_key
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### LGPD: nenhuma coluna de dado pessoal em claro nas tabelas da Ouro (resultado vazio = OK)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'akona'
# MAGIC   AND table_name LIKE 'ouro%'
# MAGIC   AND column_name IN ('cpf', 'nome', 'telefone', 'email', 'numero',
# MAGIC                       'cidadao_cpf', 'cidadao_nome', 'cidadao_telefone', 'cidadao_email');

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que o MVP consome (amostra da tabela-contrato)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT posicao_fila, id_chamado, tipo_problema, bairro, status, horas_em_aberto, sla_vencido,
# MAGIC        moradores_afetados_estimados, prioridade_score, solicitante, contato, condicao_clima
# MAGIC FROM workspace.akona.ouro_mvp_painel_chamados
# MAGIC WHERE status <> 'resolvido'
# MAGIC ORDER BY posicao_fila
# MAGIC LIMIT 10;
