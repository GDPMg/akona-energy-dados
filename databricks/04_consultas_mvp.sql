-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Akona Energy · 04 · As 5 consultas do MVP
-- MAGIC
-- MAGIC Cada consulta responde a uma pergunta que o MVP mostra para a gestão (prefeitura e distribuidora).
-- MAGIC Todas usam o modelo estrela da Ouro: `ouro_fato_chamados` + dimensões.
-- MAGIC
-- MAGIC | # | Pergunta do MVP | Tabelas |
-- MAGIC |---|---|---|
-- MAGIC | 1 | Quais bairros têm mais chamados abertos, e de que tipo? | fato + dim_bairro + dim_tipo_problema |
-- MAGIC | 2 | Qual o tempo médio de resolução por tipo de problema e por bairro? | fato + dim_tipo_problema + dim_bairro |
-- MAGIC | 3 | Quais chamados devem ser priorizados? | fato + dim_tipo_problema + dim_bairro |
-- MAGIC | 4 | Chuva e vento aumentam os chamados de queda de energia? | fato + dim_clima + dim_tipo_problema |
-- MAGIC | 5 | **Impacto:** quantos moradores são afetados por problemas não resolvidos? | fato + dim_tipo_problema + dim_bairro (IBGE) |
-- MAGIC
-- MAGIC "Abertos" ou "não resolvidos" = status `aberto` ou `em_andamento`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 1 · Quais bairros têm mais chamados abertos, e de que tipo?
-- MAGIC **Como interpretar:** o ranking mostra onde está o maior acúmulo de problemas sem solução. As colunas por tipo
-- MAGIC dizem **quem** deve agir: fio caído e queda de energia são da distribuidora; poste apagado e lâmpada piscando, da prefeitura.

-- COMMAND ----------

SELECT b.bairro,
       COUNT(*)                                          AS chamados_abertos,
       COUNT_IF(t.tipo_problema = 'fio_caido')           AS fio_caido,
       COUNT_IF(t.tipo_problema = 'queda_energia')       AS queda_energia,
       COUNT_IF(t.tipo_problema = 'poste_apagado')       AS poste_apagado,
       COUNT_IF(t.tipo_problema = 'lampada_piscando')    AS lampada_piscando,
       COUNT_IF(f.sla_vencido)                           AS com_sla_vencido
FROM workspace.akona.ouro_fato_chamados f
JOIN workspace.akona.ouro_dim_bairro b         ON b.bairro_key = f.bairro_key
JOIN workspace.akona.ouro_dim_tipo_problema t  ON t.tipo_key = f.tipo_key
WHERE f.status <> 'resolvido'
GROUP BY b.bairro
ORDER BY chamados_abertos DESC, com_sla_vencido DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 2 · Qual o tempo médio de resolução por tipo de problema e por bairro?
-- MAGIC **Como interpretar:** a linha `(todos os bairros)` dá a média de cada tipo; abaixo dela aparecem os **3 bairros mais lentos**
-- MAGIC daquele tipo. Compare `media_horas` com `sla_horas` e veja `pct_dentro_sla`: bairros com média bem acima da média do tipo
-- MAGIC indicam gargalo de equipe ou de acesso. A mediana ajuda a ver se a média está puxada por poucos casos muito demorados.

-- COMMAND ----------

WITH resolvidos AS (
  SELECT t.tipo_descricao AS tipo_problema, b.bairro, f.tempo_resolucao_horas, f.sla_vencido, t.sla_horas
  FROM workspace.akona.ouro_fato_chamados f
  JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
  JOIN workspace.akona.ouro_dim_bairro b        ON b.bairro_key = f.bairro_key
  WHERE f.status = 'resolvido'
),
agregado AS (
  SELECT tipo_problema,
         COALESCE(bairro, '(todos os bairros)')                   AS bairro_exibido,
         GROUPING(bairro)                                         AS eh_total,
         COUNT(*)                                                 AS resolvidos,
         ROUND(AVG(tempo_resolucao_horas), 1)                     AS media_horas,
         ROUND(PERCENTILE(tempo_resolucao_horas, 0.5), 1)         AS mediana_horas,
         MAX(sla_horas)                                           AS sla_horas,
         ROUND(100 * AVG(CASE WHEN sla_vencido THEN 0 ELSE 1 END), 1) AS pct_dentro_sla
  FROM resolvidos
  GROUP BY GROUPING SETS ((tipo_problema), (tipo_problema, bairro))
)
SELECT tipo_problema, bairro_exibido AS bairro, resolvidos, media_horas, mediana_horas, sla_horas, pct_dentro_sla
FROM agregado
QUALIFY eh_total = 1 OR ROW_NUMBER() OVER (PARTITION BY tipo_problema, eh_total ORDER BY media_horas DESC) <= 3
ORDER BY tipo_problema, eh_total DESC, media_horas DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 3 · Quais chamados devem ser priorizados?
-- MAGIC **Regra da fila** (`prioridade_score`): peso do tipo (fio caído 100, queda de energia 80, poste apagado 40, lâmpada 20)
-- MAGIC + até 30 pontos pelo tempo de espera + 20 se o SLA já venceu + até 20 pelo número de moradores afetados.
-- MAGIC **Como interpretar:** os primeiros da lista são os que oferecem **risco à vida** (fio caído) ou deixam **mais gente sem energia**.
-- MAGIC Um poste apagado só sobe na fila quando está esperando há muito tempo e com o SLA vencido.

-- COMMAND ----------

SELECT ROW_NUMBER() OVER (ORDER BY f.prioridade_score DESC, f.aberto_em) AS posicao,
       f.id_chamado,
       t.tipo_descricao                AS tipo_problema,
       b.bairro,
       f.status,
       f.horas_em_aberto,
       t.sla_horas,
       f.sla_vencido,
       f.moradores_afetados_estimados,
       f.prioridade_score
FROM workspace.akona.ouro_fato_chamados f
JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
JOIN workspace.akona.ouro_dim_bairro b        ON b.bairro_key = f.bairro_key
WHERE f.status <> 'resolvido'
ORDER BY posicao
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 4 · Chuva e vento aumentam os chamados de queda de energia?
-- MAGIC Cada dia é classificado pelo clima real de Osasco (Open-Meteo): **temporal** (chuva de 15 mm ou mais, ou rajada de 50 km/h ou mais),
-- MAGIC **chuva** (1 mm ou mais) ou **seco**. Comparamos a média de chamados por dia em cada condição e calculamos a correlação.
-- MAGIC **Como interpretar:** se `quedas_energia_por_dia` em dia de temporal for várias vezes maior que em dia seco, e a correlação for
-- MAGIC positiva, a resposta é **sim**. A coluna de iluminação pública funciona como **controle**: ela quase não muda com o clima.
-- MAGIC *Observação honesta:* os chamados são simulados e o simulador usa o clima real do dia como fator de risco para a rede elétrica.
-- MAGIC A consulta mostra que o pipeline captura essa relação. Com os chamados reais do webapp, a mesma consulta mediria o efeito verdadeiro.

-- COMMAND ----------

WITH por_dia AS (
  SELECT c.data, c.condicao_clima, c.precipitacao_mm, c.rajada_max_kmh,
         COUNT_IF(t.tipo_problema = 'queda_energia')    AS quedas_energia,
         COUNT_IF(t.categoria = 'Rede elétrica')        AS chamados_rede_eletrica,
         COUNT_IF(t.categoria = 'Iluminação pública')   AS chamados_iluminacao
  FROM workspace.akona.ouro_dim_clima c
  LEFT JOIN workspace.akona.ouro_fato_chamados f     ON f.data_key = c.data_key
  LEFT JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
  GROUP BY c.data, c.condicao_clima, c.precipitacao_mm, c.rajada_max_kmh
)
SELECT condicao_clima,
       COUNT(*)                                   AS dias,
       ROUND(AVG(precipitacao_mm), 1)             AS chuva_media_mm,
       ROUND(AVG(rajada_max_kmh), 1)              AS rajada_media_kmh,
       ROUND(AVG(quedas_energia), 2)              AS quedas_energia_por_dia,
       ROUND(AVG(chamados_rede_eletrica), 2)      AS rede_eletrica_por_dia,
       ROUND(AVG(chamados_iluminacao), 2)         AS iluminacao_por_dia,
       (SELECT ROUND(corr(quedas_energia, precipitacao_mm), 2) FROM por_dia) AS correlacao_chuva_quedas,
       (SELECT ROUND(corr(quedas_energia, rajada_max_kmh), 2)  FROM por_dia) AS correlacao_rajada_quedas
FROM por_dia
GROUP BY condicao_clima
ORDER BY quedas_energia_por_dia DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 5 (impacto) · Quantos moradores são afetados por problemas não resolvidos?
-- MAGIC Moradores afetados = domicílios afetados × **média de moradores por domicílio em Osasco** (IBGE, Censo 2022).
-- MAGIC A linha `TOTAL` soma tudo, e `pct_populacao_osasco` compara com a população do município (IBGE).
-- MAGIC **Como interpretar:** é o número que a gestão leva para a reunião: "hoje X mil moradores de Osasco estão sem luz na rua ou sem
-- MAGIC energia em casa por problemas ainda abertos". A coluna `com_sla_vencido` mostra quantos desses já passaram do prazo.
-- MAGIC *Limitação:* é uma estimativa. Dois chamados sobre a mesma queda podem contar as mesmas casas.

-- COMMAND ----------

WITH impacto AS (
  SELECT COALESCE(t.tipo_descricao, 'TOTAL')                                         AS tipo_problema,
         COUNT(*)                                                                     AS chamados_nao_resolvidos,
         SUM(f.domicilios_afetados)                                                   AS domicilios_afetados,
         SUM(f.moradores_afetados_estimados)                                          AS moradores_afetados,
         ROUND(100.0 * SUM(f.moradores_afetados_estimados) / MAX(b.populacao_municipio), 2) AS pct_populacao_osasco,
         COUNT_IF(f.sla_vencido)                                                      AS com_sla_vencido
  FROM workspace.akona.ouro_fato_chamados f
  JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
  JOIN workspace.akona.ouro_dim_bairro b        ON b.bairro_key = f.bairro_key
  WHERE f.status <> 'resolvido'
  GROUP BY ROLLUP (t.tipo_descricao)
)
SELECT * FROM impacto
ORDER BY tipo_problema = 'TOTAL', moradores_afetados DESC;
