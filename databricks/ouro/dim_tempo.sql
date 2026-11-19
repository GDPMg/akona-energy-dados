-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · dim_tempo
-- MAGIC Calendário com um registro por dia, do primeiro ao último dia com dados (chamados ou clima).
-- MAGIC Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_dim_tempo
COMMENT 'Dimensão calendário (um registro por dia)'
AS
WITH limites AS (
  SELECT LEAST(c.inicio, k.inicio) AS inicio, GREATEST(c.fim, k.fim) AS fim
  FROM (SELECT MIN(DATE(aberto_em)) AS inicio, MAX(DATE(atualizado_em)) AS fim FROM workspace.akona.prata_chamados) c
  CROSS JOIN (SELECT MIN(DATE(data_hora)) AS inicio, MAX(DATE(data_hora)) AS fim FROM workspace.akona.prata_clima_horario) k
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
FROM dias;

-- COMMAND ----------

SELECT COUNT(*) AS dias, MIN(data) AS primeiro_dia, MAX(data) AS ultimo_dia FROM workspace.akona.ouro_dim_tempo;
