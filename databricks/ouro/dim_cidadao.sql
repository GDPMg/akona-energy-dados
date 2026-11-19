-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · dim_cidadao (LGPD)
-- MAGIC Cidadão **pseudonimizado**: o CPF vira `sha2(salt + cpf, 256)`. O salt fica no Databricks Secrets
-- MAGIC (escopo `akona`, chave `salt_lgpd`), foi gerado aleatoriamente pelo Airflow e nunca vai para o repositório.
-- MAGIC Sem salt, daria para descobrir o CPF testando os ~10⁹ CPFs possíveis. Nome e telefone saem mascarados.
-- MAGIC Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_dim_cidadao
COMMENT 'Dimensão cidadão PSEUDONIMIZADA (LGPD): hash do CPF com salt secreto, nome e telefone mascarados'
AS
WITH ultimo AS (
  SELECT cpf,
         max_by(nome, atualizado_em) AS nome,
         max_by(telefone, atualizado_em) AS telefone,
         COUNT(*) AS total_chamados
  FROM workspace.akona.prata_chamados
  WHERE cpf IS NOT NULL
  GROUP BY cpf
)
SELECT sha2(concat(secret('akona', 'salt_lgpd'), cpf), 256) AS cidadao_key,
       array_join(transform(split(nome, ' '), p -> concat(left(p, 1), repeat('*', length(p) - 1))), ' ') AS nome_mascarado,
       concat('(', substr(regexp_replace(telefone, '[^0-9]', ''), 1, 2), ') *****-',
              right(regexp_replace(telefone, '[^0-9]', ''), 4)) AS telefone_mascarado,
       total_chamados
FROM ultimo;

-- COMMAND ----------

SELECT * FROM workspace.akona.ouro_dim_cidadao LIMIT 5;
