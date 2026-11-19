-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · mvp_painel_chamados (contrato com o produto)
-- MAGIC Tabela pronta para exibição no MVP: chamados com rótulos, posição na fila de prioridade e
-- MAGIC só dados pessoais **mascarados**. Executado pela DAG `akona_ouro`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.akona.ouro_mvp_painel_chamados
COMMENT 'CONTRATO DE DADOS DO MVP Akona Energy: única tabela lida pelo painel. Atualização diária. Sem CPF, nome, telefone ou e-mail em claro.'
AS
SELECT f.id_chamado,
       tm.data AS data_abertura,
       f.aberto_em,
       f.atualizado_em,
       f.status,
       t.tipo_descricao AS tipo_problema,
       t.categoria,
       t.responsavel,
       b.bairro,
       f.logradouro,
       f.horas_em_aberto,
       f.tempo_resolucao_horas,
       t.sla_horas,
       f.sla_vencido,
       f.prioridade_score,
       CASE WHEN f.status <> 'resolvido'
            THEN ROW_NUMBER() OVER (PARTITION BY f.status = 'resolvido'
                                    ORDER BY f.prioridade_score DESC, f.aberto_em) END AS posicao_fila,
       f.domicilios_afetados,
       f.moradores_afetados_estimados,
       ci.nome_mascarado AS solicitante,
       ci.telefone_mascarado AS contato,
       cl.condicao_clima,
       cl.precipitacao_mm AS chuva_no_dia_mm,
       cl.rajada_max_kmh AS rajada_no_dia_kmh,
       f.latitude_aprox,
       f.longitude_aprox
FROM workspace.akona.ouro_fato_chamados f
JOIN workspace.akona.ouro_dim_tempo tm ON tm.data_key = f.data_key
JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
LEFT JOIN workspace.akona.ouro_dim_cidadao ci ON ci.cidadao_key = f.cidadao_key
LEFT JOIN workspace.akona.ouro_dim_clima cl ON cl.data_key = f.data_key;

-- COMMAND ----------

SELECT posicao_fila, id_chamado, tipo_problema, bairro, status, horas_em_aberto, sla_vencido,
       moradores_afetados_estimados, prioridade_score, solicitante, contato, condicao_clima
FROM workspace.akona.ouro_mvp_painel_chamados
WHERE status <> 'resolvido'
ORDER BY posicao_fila
LIMIT 10;
