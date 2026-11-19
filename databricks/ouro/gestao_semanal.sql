-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ouro · gestao_semanal (tela "Painel da gestão" → tabela "Últimas 12 semanas")
-- MAGIC Chamados abertos e resolvidos em cada uma das últimas 12 semanas, contadas para trás a partir da data de
-- MAGIC referência. Semanas anteriores ao início dos dados (01/10/2026) não aparecem. Campos iguais a `semanal` do `/api/indicadores`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.akona.ouro_gestao_semanal
COMMENT 'Tela Painel da gestão do MVP: chamados abertos e resolvidos nas últimas 12 semanas'
AS
WITH referencia AS (
  SELECT CAST(MAX(atualizado_em) AS DATE) AS hoje, CAST(MIN(aberto_em) AS DATE) AS inicio_dados
  FROM workspace.akona.ouro_fato_chamados
),
semanas AS (
  SELECT date_sub(r.hoje, 7 * k + 6) AS inicio, date_sub(r.hoje, 7 * k) AS fim
  FROM referencia r LATERAL VIEW explode(sequence(0, 11)) s AS k
  WHERE date_sub(r.hoje, 7 * k) >= r.inicio_dados
),
abertos AS (
  SELECT s.inicio, COUNT(f.id_chamado) AS n
  FROM semanas s LEFT JOIN workspace.akona.ouro_fato_chamados f
    ON CAST(f.aberto_em AS DATE) BETWEEN s.inicio AND s.fim
  GROUP BY s.inicio
),
resolvidos AS (
  SELECT s.inicio, COUNT(f.id_chamado) AS n
  FROM semanas s LEFT JOIN workspace.akona.ouro_fato_chamados f
    ON CAST(f.resolvido_em AS DATE) BETWEEN s.inicio AND s.fim
  GROUP BY s.inicio
)
SELECT s.inicio AS inicio, s.fim, a.n AS abertos, r.n AS resolvidos
FROM semanas s
JOIN abertos a USING (inicio)
JOIN resolvidos r USING (inicio);

-- COMMAND ----------

SELECT * FROM workspace.akona.ouro_gestao_semanal ORDER BY inicio;
