-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Akona Energy · verificação de LGPD e resumo da Ouro
-- MAGIC Prova de que a camada Ouro não tem dado pessoal em claro. Executado pela DAG `akona_consultas`.

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Linhas por tabela da Ouro

-- COMMAND ----------

SELECT 'ouro_fato_chamados' AS tabela, COUNT(*) AS linhas FROM workspace.akona.ouro_fato_chamados
UNION ALL SELECT 'ouro_dim_tempo', COUNT(*) FROM workspace.akona.ouro_dim_tempo
UNION ALL SELECT 'ouro_dim_clima', COUNT(*) FROM workspace.akona.ouro_dim_clima
UNION ALL SELECT 'ouro_dim_bairro', COUNT(*) FROM workspace.akona.ouro_dim_bairro
UNION ALL SELECT 'ouro_dim_tipo_problema', COUNT(*) FROM workspace.akona.ouro_dim_tipo_problema
UNION ALL SELECT 'ouro_dim_cidadao', COUNT(*) FROM workspace.akona.ouro_dim_cidadao
UNION ALL SELECT 'ouro_mvp_painel_chamados', COUNT(*) FROM workspace.akona.ouro_mvp_painel_chamados;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Antes (Prata, acesso restrito) × depois (Ouro). Dados 100% fictícios.

-- COMMAND ----------

SELECT p.id_chamado,
       p.nome AS nome_prata, c.nome_mascarado AS nome_ouro,
       p.cpf AS cpf_prata, f.cidadao_key AS cpf_hash_ouro,
       p.telefone AS telefone_prata, c.telefone_mascarado AS telefone_ouro,
       concat(p.logradouro, ', ', p.numero) AS endereco_prata, f.logradouro AS endereco_ouro
FROM workspace.akona.prata_chamados p
JOIN workspace.akona.ouro_fato_chamados f USING (id_chamado)
JOIN workspace.akona.ouro_dim_cidadao c ON c.cidadao_key = f.cidadao_key
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Nenhuma coluna de dado pessoal em claro nas tabelas da Ouro (resultado vazio = OK)

-- COMMAND ----------

SELECT table_name, column_name
FROM workspace.information_schema.columns
WHERE table_schema = 'akona'
  AND table_name LIKE 'ouro%'
  AND column_name IN ('cpf', 'nome', 'telefone', 'email', 'numero',
                      'cidadao_cpf', 'cidadao_nome', 'cidadao_telefone', 'cidadao_email');
