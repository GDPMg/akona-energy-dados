-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · gestao_fila (tela "Painel da gestão" → "Fila de chamados não resolvidos")
-- MAGIC Chamados ainda não resolvidos, com o **ponto** (poste) vindo da ordem de serviço da manutenção e a **ação** do botão
-- MAGIC (Iniciar para aberto, Resolver para em andamento). Fio caído e queda de energia são da distribuidora e não têm poste.
-- MAGIC Sem dados pessoais.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.akona.ouro_gestao_fila
COMMENT 'Tela Painel da gestão do MVP: fila de chamados não resolvidos, com o poste (ponto) e a ação do botão'
AS
SELECT f.id_chamado                                   AS protocolo,
       lower(t.tipo_descricao)                        AS tipo,
       b.bairro,
       m.ponto_id                                     AS ponto,
       f.aberto_em,
       replace(f.status, '_', ' ')                    AS status,
       CASE f.status WHEN 'aberto' THEN 'Iniciar' WHEN 'em_andamento' THEN 'Resolver' END AS acao,
       f.horas_em_aberto,
       f.sla_vencido,
       f.prioridade_score
FROM workspace.akona.ouro_fato_chamados f
JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
LEFT JOIN workspace.akona.ouro_fato_manutencao m ON m.id_chamado = f.id_chamado
WHERE f.status <> 'resolvido';

-- COMMAND ----------

SELECT protocolo, tipo, bairro, ponto, aberto_em, status, acao
FROM workspace.akona.ouro_gestao_fila ORDER BY aberto_em DESC LIMIT 10;
