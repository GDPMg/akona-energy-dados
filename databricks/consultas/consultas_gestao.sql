-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Akona Energy · consultas da tela "Painel da gestão" do MVP web
-- MAGIC
-- MAGIC São as consultas que o web app faria (via SQL Warehouse) para montar a tela `/gestao`. Cada bloco da tela lê
-- MAGIC **uma view da Ouro**. O filtro "últimos 30 / 90 dias" da tela vira `WHERE dias = 30` ou `WHERE dias = 90`
-- MAGIC (aqui usamos 90, o padrão da tela). Executado pela DAG `akona_consultas`. Linhagem completa em `docs/mvp_gestao.md`.
-- MAGIC
-- MAGIC | Bloco da tela | View da Ouro |
-- MAGIC |---|---|
-- MAGIC | Cartões (abertos, tempo médio de reparo, no prazo, gap de equidade, reincidência) | `ouro_gestao_indicadores` |
-- MAGIC | Fila de chamados não resolvidos | `ouro_gestao_fila` |
-- MAGIC | Por bairro | `ouro_gestao_por_bairro` |
-- MAGIC | Últimas 12 semanas | `ouro_gestao_semanal` |
-- MAGIC | Endpoint `/api/indicadores` (JSON) | as três views acima, montadas no mesmo formato |

-- COMMAND ----------

SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Cartões do topo

-- COMMAND ----------

SELECT dias, total, abertos, tempo_medio_reparo_horas, percentual_no_prazo, meta_percentual_no_prazo,
       gap_equidade, meta_gap_equidade, percentual_reincidencia, meta_percentual_reincidencia, data_referencia
FROM workspace.akona.ouro_gestao_indicadores
WHERE dias = 90;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Fila de chamados não resolvidos (mais recentes primeiro)

-- COMMAND ----------

SELECT protocolo, tipo, bairro, COALESCE(ponto, '—') AS ponto, aberto_em, status, acao
FROM workspace.akona.ouro_gestao_fila
ORDER BY aberto_em DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Por bairro (do mais lento para o mais rápido)

-- COMMAND ----------

SELECT bairro, chamados, tempo_medio_horas, percentual_no_prazo, barra_percentual
FROM workspace.akona.ouro_gestao_por_bairro
WHERE dias = 90
ORDER BY ordem;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Últimas 12 semanas

-- COMMAND ----------

SELECT inicio AS semana_a_partir_de, abertos, resolvidos
FROM workspace.akona.ouro_gestao_semanal
ORDER BY inicio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. O mesmo JSON do endpoint `/api/indicadores?dias=90` do MVP
-- MAGIC Prova de que a Ouro entrega exatamente o contrato que a tela consome (mesmos nomes de campo).

-- COMMAND ----------

SELECT to_json(named_struct(
         'dias', i.dias,
         'total', i.total,
         'abertos', i.abertos,
         'tempo_medio_reparo_horas', i.tempo_medio_reparo_horas,
         'percentual_no_prazo', i.percentual_no_prazo,
         'gap_equidade', i.gap_equidade,
         'percentual_reincidencia', i.percentual_reincidencia,
         'por_bairro', (SELECT transform(
                                 array_sort(collect_list(struct(b.ordem, b.bairro, b.chamados, b.tempo_medio_horas,
                                                                b.percentual_no_prazo, b.barra_percentual))),
                                 x -> named_struct('bairro', x.bairro, 'chamados', x.chamados,
                                                   'tempo_medio_horas', x.tempo_medio_horas,
                                                   'percentual_no_prazo', x.percentual_no_prazo,
                                                   'barra_percentual', x.barra_percentual))
                        FROM workspace.akona.ouro_gestao_por_bairro b WHERE b.dias = i.dias),
         'semanal', (SELECT transform(array_sort(collect_list(struct(s.inicio, s.abertos, s.resolvidos))),
                                      x -> named_struct('inicio', CAST(x.inicio AS STRING), 'abertos', x.abertos,
                                                        'resolvidos', x.resolvidos))
                     FROM workspace.akona.ouro_gestao_semanal s)
       )) AS json_api_indicadores
FROM workspace.akona.ouro_gestao_indicadores i
WHERE i.dias = 90;
