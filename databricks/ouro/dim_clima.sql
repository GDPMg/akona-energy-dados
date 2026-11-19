-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · dim_clima
-- MAGIC Agrega as 24 horas de cada dia (Open-Meteo) e classifica a condição do dia:
-- MAGIC **temporal** (chuva de 15 mm ou mais, ou rajada de 50 km/h ou mais), **chuva** (1 mm ou mais) ou **seco**.
-- MAGIC Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_dim_clima
COMMENT 'Dimensão clima diário de Osasco (Open-Meteo). Temporal = chuva de 15 mm ou mais no dia ou rajada de 50 km/h ou mais'
AS
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
FROM workspace.akona.prata_clima_horario
GROUP BY DATE(data_hora);

-- COMMAND ----------

SELECT condicao_clima, COUNT(*) AS dias FROM workspace.akona.ouro_dim_clima GROUP BY condicao_clima ORDER BY dias DESC;
