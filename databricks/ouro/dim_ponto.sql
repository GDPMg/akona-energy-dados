-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · dim_ponto
-- MAGIC Pontos de iluminação (postes) que já tiveram ordem de serviço: bairro, tecnologia (LED ou vapor de sódio) e potência.
-- MAGIC Fonte: `prata_manutencao`. Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_dim_ponto
COMMENT 'Dimensão ponto de iluminação (poste): bairro, tecnologia, potência e histórico de ordens de serviço'
AS
SELECT ponto_id,
       max_by(bairro, atualizado_em)      AS bairro,
       max_by(tecnologia, atualizado_em)  AS tecnologia,
       max_by(potencia_w, atualizado_em)  AS potencia_w,
       MIN(aberta_em)                     AS primeira_os_em,
       COUNT(*)                           AS total_os
FROM workspace.akona.prata_manutencao
GROUP BY ponto_id;

-- COMMAND ----------

SELECT tecnologia, COUNT(*) AS postes, SUM(total_os) AS ordens_de_servico
FROM workspace.akona.ouro_dim_ponto GROUP BY tecnologia;
