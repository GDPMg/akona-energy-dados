-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · gestao_indicadores (tela "Painel da gestão" → cartões do topo)
-- MAGIC Uma linha por janela do filtro (30 e 90 dias) com os 5 cartões da tela. Os nomes seguem os campos do
-- MAGIC endpoint `/api/indicadores` do MVP.
-- MAGIC
-- MAGIC | Cartão da tela | Coluna | Cálculo |
-- MAGIC |---|---|---|
-- MAGIC | Chamados abertos "x de y no período" | `abertos` / `total` | não resolvidos / abertos na janela |
-- MAGIC | Tempo médio de reparo | `tempo_medio_reparo_horas` | média abertura → resolução dos resolvidos |
-- MAGIC | No prazo (meta 85%) | `percentual_no_prazo` | % dos resolvidos dentro do SLA do tipo |
-- MAGIC | Gap de equidade (meta < 1,5) | `gap_equidade` | tempo médio do bairro mais lento ÷ tempo médio da cidade |
-- MAGIC | Reincidência (meta < 10%) | `percentual_reincidencia` | % das ordens de serviço em poste que já teve OS nos 30 dias anteriores |

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.akona.ouro_gestao_indicadores
COMMENT 'Tela Painel da gestão do MVP: cartões de indicadores por janela (30 e 90 dias). Campos iguais a /api/indicadores.'
AS
WITH cidade AS (
  SELECT janela_dias, MAX(data_referencia) AS data_referencia,
         COUNT(*)                                  AS total,
         COUNT_IF(status <> 'resolvido')           AS abertos,
         AVG(tempo_resolucao_horas)                AS tempo_medio,
         100 * AVG(CAST(resolvido_no_prazo AS INT)) AS no_prazo
  FROM workspace.akona.ouro_gestao_chamados_periodo
  GROUP BY janela_dias
),
bairros AS (
  SELECT janela_dias, bairro, AVG(tempo_resolucao_horas) AS tempo_bairro
  FROM workspace.akona.ouro_gestao_chamados_periodo
  GROUP BY janela_dias, bairro
),
equidade AS (SELECT janela_dias, MAX(tempo_bairro) AS bairro_mais_lento FROM bairros GROUP BY janela_dias),
reincidencia AS (
  SELECT c.janela_dias, 100 * AVG(CAST(m.reincidente_30d AS INT)) AS pct
  FROM cidade c
  JOIN workspace.akona.ouro_fato_manutencao m
    ON CAST(m.aberta_em AS DATE) > date_sub(c.data_referencia, c.janela_dias)
  GROUP BY c.janela_dias
)
SELECT c.janela_dias                                      AS dias,
       c.data_referencia,
       c.total,
       c.abertos,
       ROUND(c.tempo_medio, 1)                            AS tempo_medio_reparo_horas,
       ROUND(c.no_prazo, 1)                               AS percentual_no_prazo,
       ROUND(e.bairro_mais_lento / c.tempo_medio, 2)      AS gap_equidade,
       ROUND(r.pct, 1)                                    AS percentual_reincidencia,
       85                                                 AS meta_percentual_no_prazo,
       1.5                                                AS meta_gap_equidade,
       10                                                 AS meta_percentual_reincidencia
FROM cidade c
JOIN equidade e USING (janela_dias)
LEFT JOIN reincidencia r USING (janela_dias);

-- COMMAND ----------

SELECT * FROM workspace.akona.ouro_gestao_indicadores ORDER BY dias;
