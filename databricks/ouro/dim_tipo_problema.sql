-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · dim_tipo_problema
-- MAGIC Dimensão de referência definida pela equipe: categoria, responsável, gravidade, peso na fila de prioridade e
-- MAGIC SLA de referência do projeto. Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_dim_tipo_problema
COMMENT 'Dimensão tipo de problema: categoria, responsável, gravidade, peso na fila de prioridade e SLA de referência'
AS SELECT * FROM VALUES
  (1, 'fio_caido',        'Fio caído',        'Rede elétrica',      'Distribuidora de energia', 4, 100,   8),
  (2, 'queda_energia',    'Queda de energia', 'Rede elétrica',      'Distribuidora de energia', 3,  80,  24),
  (3, 'poste_apagado',    'Poste apagado',    'Iluminação pública', 'Prefeitura de Osasco',     2,  40, 120),
  (4, 'lampada_piscando', 'Lâmpada piscando', 'Iluminação pública', 'Prefeitura de Osasco',     1,  20, 168)
AS t(tipo_key, tipo_problema, tipo_descricao, categoria, responsavel, gravidade, peso_prioridade, sla_horas);

-- COMMAND ----------

SELECT * FROM workspace.akona.ouro_dim_tipo_problema ORDER BY tipo_key;
