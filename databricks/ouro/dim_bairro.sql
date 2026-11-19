-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · dim_bairro
-- MAGIC Bairros que aparecem nos chamados, enriquecidos com indicadores do município vindos do IBGE (Censo 2022):
-- MAGIC população e média de moradores por domicílio (usada para estimar moradores afetados). Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_dim_bairro
COMMENT 'Dimensão bairro de Osasco, enriquecida com indicadores do IBGE (Censo 2022)'
AS
WITH ibge AS (
  SELECT MAX(cod_municipio) AS cod_ibge_municipio,
         MAX(municipio) AS municipio,
         MAX(uf) AS uf,
         MAX(CASE WHEN indicador_id = '93'   THEN valor END) AS populacao_municipio,
         MAX(CASE WHEN indicador_id = '381'  THEN valor END) AS domicilios_municipio,
         MAX(CASE WHEN indicador_id = '5930' THEN valor END) AS media_moradores_domicilio,
         MAX(CASE WHEN indicador_id = '614'  THEN valor END) AS densidade_hab_km2
  FROM workspace.akona.prata_ibge_indicadores
  WHERE ano = '2022'
)
SELECT xxhash64(b.bairro) AS bairro_key,
       b.bairro, i.municipio, i.uf, i.cod_ibge_municipio,
       CAST(i.populacao_municipio AS BIGINT) AS populacao_municipio,
       CAST(i.domicilios_municipio AS BIGINT) AS domicilios_municipio,
       i.media_moradores_domicilio,
       i.densidade_hab_km2
FROM (SELECT DISTINCT bairro FROM workspace.akona.prata_chamados) b
CROSS JOIN ibge i;

-- COMMAND ----------

SELECT bairro, municipio, uf, populacao_municipio, media_moradores_domicilio
FROM workspace.akona.ouro_dim_bairro ORDER BY bairro;
