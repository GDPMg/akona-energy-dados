-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · fato_chamados
-- MAGIC Uma linha por chamado (último estado). Executado pela DAG `akona_ouro`, depois de `dim_bairro` e `dim_tipo_problema`.
-- MAGIC
-- MAGIC - `tempo_resolucao_horas`: só para resolvidos. `horas_em_aberto`: para não resolvidos, até a **data de referência**
-- MAGIC   (o evento mais recente da base).
-- MAGIC - `moradores_afetados_estimados` = domicílios afetados × média de moradores por domicílio em Osasco (IBGE, Censo 2022).
-- MAGIC - `prioridade_score` (só não resolvidos) = **peso do tipo** (fio 100, queda 80, poste 40, lâmpada 20)
-- MAGIC   + até **30** pelo tempo de espera (teto de 7 dias) + **20** se o SLA venceu + até **20** pelo impacto (teto de 2.000 moradores).
-- MAGIC - LGPD: `cidadao_key` é o mesmo hash da `dim_cidadao`; logradouro sem número e coordenadas arredondadas (~100 m).

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_fato_chamados
COMMENT 'Fato: um registro por chamado (último estado), com tempos, impacto, SLA e prioridade. Sem dados pessoais em claro.'
AS
WITH referencia AS (
  SELECT MAX(atualizado_em) AS data_referencia FROM workspace.akona.prata_chamados
),
base AS (
  SELECT c.id_chamado,
         CAST(date_format(c.aberto_em, 'yyyyMMdd') AS INT) AS data_key,
         b.bairro_key,
         t.tipo_key,
         sha2(concat(secret('akona', 'salt_lgpd'), c.cpf), 256) AS cidadao_key,
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
  FROM workspace.akona.prata_chamados c
  CROSS JOIN referencia r
  JOIN workspace.akona.ouro_dim_bairro b ON b.bairro = c.bairro
  JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_problema = c.tipo_problema
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
FROM base;

-- COMMAND ----------

SELECT status, COUNT(*) AS chamados, SUM(moradores_afetados_estimados) AS moradores_afetados
FROM workspace.akona.ouro_fato_chamados GROUP BY status ORDER BY chamados DESC;
