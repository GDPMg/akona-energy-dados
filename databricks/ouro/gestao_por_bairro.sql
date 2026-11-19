-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · gestao_por_bairro (tela "Painel da gestão" → tabela "Por bairro")
-- MAGIC Por janela (30 e 90 dias) e bairro: chamados, tempo médio de reparo e % no prazo, do mais lento para o mais rápido.
-- MAGIC `barra_percentual` é o tamanho da barra da tela (100 = bairro mais lento). Campos iguais a `por_bairro` do `/api/indicadores`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.akona.ouro_gestao_por_bairro
COMMENT 'Tela Painel da gestão do MVP: tabela Por bairro (do mais lento ao mais rápido), por janela de 30 e 90 dias'
AS
WITH agregado AS (
  SELECT janela_dias, bairro,
         COUNT(*)                                   AS chamados,
         AVG(tempo_resolucao_horas)                 AS tempo_medio,
         100 * AVG(CAST(resolvido_no_prazo AS INT)) AS no_prazo
  FROM workspace.akona.ouro_gestao_chamados_periodo
  GROUP BY janela_dias, bairro
)
SELECT janela_dias                                                                            AS dias,
       ROW_NUMBER() OVER (PARTITION BY janela_dias ORDER BY tempo_medio DESC NULLS LAST)      AS ordem,
       bairro,
       chamados,
       ROUND(tempo_medio, 1)                                                                  AS tempo_medio_horas,
       ROUND(no_prazo, 1)                                                                     AS percentual_no_prazo,
       CAST(ROUND(100 * tempo_medio / MAX(tempo_medio) OVER (PARTITION BY janela_dias)) AS INT) AS barra_percentual
FROM agregado;

-- COMMAND ----------

SELECT * FROM workspace.akona.ouro_gestao_por_bairro WHERE dias = 90 ORDER BY ordem;
