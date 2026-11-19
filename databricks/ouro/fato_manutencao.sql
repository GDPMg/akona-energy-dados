-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · fato_manutencao
-- MAGIC Uma linha por **ordem de serviço** da manutenção da iluminação pública (último estado).
-- MAGIC Executado pela DAG `akona_ouro`, depois de `dim_bairro`.
-- MAGIC
-- MAGIC - `reincidente_30d`: o mesmo poste teve outra ordem de serviço aberta nos 30 dias anteriores (indicador de
-- MAGIC   **reincidência** da tela Painel da gestão; meta: abaixo de 10%).
-- MAGIC - `horas_ate_atendimento`: da abertura até a equipe começar; `horas_ate_conclusao`: da abertura até concluir.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_fato_manutencao
COMMENT 'Fato: uma linha por ordem de serviço da iluminação pública, com tempos de atendimento e reincidência no poste'
AS
WITH os AS (
  SELECT *,
         LAG(aberta_em) OVER (PARTITION BY ponto_id ORDER BY aberta_em) AS os_anterior_no_poste_em
  FROM workspace.akona.prata_manutencao
)
SELECT o.os_id,
       o.id_chamado,
       o.ponto_id,
       CAST(date_format(o.aberta_em, 'yyyyMMdd') AS INT) AS data_key,
       b.bairro_key,
       o.status,
       o.tipo_servico,
       o.equipe,
       o.aberta_em,
       o.iniciada_em,
       o.concluida_em,
       ROUND(CAST(timestampdiff(MINUTE, o.aberta_em, o.iniciada_em) AS DOUBLE) / 60, 1)  AS horas_ate_atendimento,
       ROUND(CAST(timestampdiff(MINUTE, o.aberta_em, o.concluida_em) AS DOUBLE) / 60, 1) AS horas_ate_conclusao,
       COALESCE(o.os_anterior_no_poste_em >= o.aberta_em - INTERVAL 30 DAYS, false)       AS reincidente_30d
FROM os o
LEFT JOIN workspace.akona.ouro_dim_bairro b ON b.bairro = o.bairro;

-- COMMAND ----------

SELECT status, COUNT(*) AS ordens, ROUND(100 * AVG(CAST(reincidente_30d AS INT)), 1) AS pct_reincidencia
FROM workspace.akona.ouro_fato_manutencao GROUP BY status ORDER BY ordens DESC;
