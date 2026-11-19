-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · gestao_chamados_periodo (view base da tela "Painel da gestão")
-- MAGIC Chamados abertos dentro de cada janela do filtro da tela (**últimos 30** e **últimos 90 dias**), contados até a
-- MAGIC data de referência (o evento mais recente da base). As views `gestao_indicadores` e `gestao_por_bairro` partem daqui.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.akona.ouro_gestao_chamados_periodo
COMMENT 'Base da tela Painel da gestão do MVP: chamados de cada janela do filtro (30 e 90 dias)'
AS
WITH referencia AS (SELECT MAX(atualizado_em) AS agora FROM workspace.akona.ouro_fato_chamados),
janelas AS (SELECT janela_dias FROM VALUES (30), (90) AS j(janela_dias))
SELECT j.janela_dias,
       CAST(r.agora AS DATE)                                              AS data_referencia,
       f.id_chamado,
       b.bairro,
       t.tipo_problema,
       f.status,
       f.aberto_em,
       f.resolvido_em,
       f.tempo_resolucao_horas,
       CASE WHEN f.status = 'resolvido' THEN NOT f.sla_vencido END        AS resolvido_no_prazo
FROM janelas j
CROSS JOIN referencia r
JOIN workspace.akona.ouro_fato_chamados f ON f.aberto_em > timestampadd(DAY, -j.janela_dias, r.agora)
JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key;

-- COMMAND ----------

SELECT janela_dias, data_referencia, COUNT(*) AS chamados
FROM workspace.akona.ouro_gestao_chamados_periodo GROUP BY ALL ORDER BY janela_dias;
