<!--
COMO USAR ESTE MODELO (apague este bloco antes de exportar)
- As seções seguem a ordem pedida no enunciado. Não mude a ordem (relatório fora da ordem perde até 1,0 ponto).
- Troque cada [PRINT: ...] pela imagem e mantenha a legenda "Figura N – ..." logo abaixo. Prints sem legenda ou ilegíveis também perdem pontos.
- Troque os valores entre colchetes [ ] pelos números reais das execuções de vocês.
- Para gerar o PDF: copie para o Word ou o Google Docs e exporte em PDF, ou use a extensão "Markdown PDF" do VS Code.
- Preencha nome completo e RM de TODOS os integrantes (faltar alguém custa 0,5 ponto).
-->

# Akona Energy · Componente de Dados do MVP

**Global Solution FIAP 2026.2 · CitizenTech**
**Disciplina:** Big Data Architecture & Data Integration · Engenharia de Software, 4º ano

**Repositório:** [https://github.com/USUARIO/REPOSITORIO](https://github.com/USUARIO/REPOSITORIO)

---

## 1. Integrantes

| Nome completo | RM |
|---|---|
| [nome completo] | [RM] |
| [nome completo] | [RM] |
| [nome completo] | [RM] |

## 2. O produto e as perguntas que o MVP responde

**O produto em uma frase:** o Akona Energy é um webapp em que moradores de Osasco (SP) reportam problemas de
iluminação pública e de energia (poste apagado, lâmpada piscando, fio caído e queda de energia) para que a prefeitura e
a distribuidora acompanhem, priorizem e resolvam os chamados.

Este trabalho entrega o **componente de Dados** do MVP. A tela do MVP é um painel para a gestão que responde:

1. Quais bairros têm mais chamados abertos, e de que tipo?
2. Qual o tempo médio de resolução por tipo de problema e por bairro?
3. Quais chamados devem ser priorizados (fio caído e queda de energia primeiro)?
4. Chuva e vento aumentam os chamados de queda de energia?
5. **Impacto:** quantos moradores são afetados por problemas não resolvidos?

## 3. Diagrama do fluxo de dados, da fonte ao MVP

![Fluxo de dados](img/arquitetura.png)

*Figura 1 – Fluxo de dados do Akona Energy: as fontes são coletadas pelo Airflow para a pasta chegada/, enviadas em lotes ao Volume do Databricks e tratadas nas camadas Bronze, Prata e Ouro até a tela do MVP.*

O Airflow roda em Docker no computador da equipe e grava os arquivos na pasta `chegada/`. Como o Databricks Free
Edition não se conecta ao Airflow, os arquivos sobem ao Volume `workspace.akona.chegada` pela interface, em lotes. A partir
daí, o Auto Loader carrega a Bronze, a Prata trata e valida, a Ouro organiza em modelo estrela e protege os dados pessoais,
e o painel AI/BI lê só a tabela-contrato `ouro_mvp_painel_chamados`.

## 4. Tabela de fontes

| Fonte | Origem | Formato | Frequência de coleta |
|---|---|---|---|
| Clima de Osasco (**externa**) | API pública Open-Meteo (`api.open-meteo.com/v1/forecast`), sem chave: chuva, vento, rajada, temperatura e código do tempo, por hora | JSON | diária, 06:00 (dia anterior) |
| Município e Censo 2022 (**externa**) | API pública do IBGE: localidades + agregados 4714 (população, área, densidade) e 4712 (domicílios e média de moradores por domicílio) | JSON | diária (dado de referência) |
| Chamados dos moradores (interna) | API do webapp Akona, **simulada** em Python porque o webapp ainda não existe. Exige token e usa dados pessoais fictícios | JSON Lines | diária, 06:00 (dia anterior) |

A ANEEL foi avaliada, mas o dataset de interrupções só é publicado como arquivos anuais nacionais de 265 a 424 MB,
com atualização mensal e sem API de consulta. Por isso ficou como trabalho futuro.

## 5. Requisitos 01 a 09

### Requisito 01 · Fontes de dados
Foram escolhidas duas fontes externas reais e uma interna simulada, todas ligadas ao problema. O **Open-Meteo** traz o
clima de Osasco, usado para medir se chuva e vento aumentam as quedas de energia. O **IBGE** traz a população e a média
de moradores por domicílio (2,78 no Censo 2022), usadas para estimar quantos moradores cada chamado afeta. Os
**chamados** vêm de um simulador que imita a API do webapp.

[PRINT: a pasta `amostras/` no GitHub ou um trecho de cada arquivo de amostra]
*Figura 2 – Amostras das três fontes no repositório.*

### Requisito 02 · DAG agendado gravando na pasta chegada/
O DAG `akona_coleta_diaria` está agendado para todo dia às 06:00 (`0 6 * * *`) e coleta o dia anterior. Com
`catchup=True` e início em 02/10/2026, o Airflow executou um dia de cada vez desde 01/10. Cada execução grava um arquivo
por fonte com a data no nome, por exemplo `clima_2026-10-05.json`.

[PRINT: a pasta `chegada/` aberta no Explorer, mostrando os arquivos datados das três subpastas]
*Figura 3 – Arquivos gravados pelo DAG em chegada/, um por fonte e por dia de coleta.*

### Requisito 03 · Paralelismo, conferência da chegada e chaves fora do código
`coletar_clima` e `coletar_ibge` rodam **em paralelo** depois de `inicio`. `simular_chamados` espera o clima, porque usa
a chuva do dia. A tarefa `conferir_chegada` verifica se os três arquivos do dia existem, não estão vazios e são JSON
válido; se faltar algum, ela falha e o `on_failure_callback` dispara a notificação. No fim, `notificar` registra o
resumo (e envia ao Discord, se o webhook estiver configurado). O token da API fica na Variable `akona_api_token`, que o
Airflow mascara na tela, e nada sensível aparece no código.

[PRINT: Admin → Variables com akona_api_token mascarada]
*Figura 4 – Variables do Airflow: o token da API fica fora do código e aparece mascarado.*

[PRINT: log da tarefa notificar mostrando a mensagem "coleta de AAAA-MM-DD concluída"]
*Figura 5 – Notificação de sucesso registrada pela tarefa notificar.*

### Requisito 04 · Dois ou mais lotes enviados ao Volume
Os arquivos foram enviados ao Volume `workspace.akona.chegada` pela interface do Databricks em dois lotes: **lote 1**
com os dias de outubro (01/10 a 31/10) e **lote 2** com os dias de novembro (01/11 a [DD]/11).

[PRINT: Catalog → Volumes → chegada → chamados mostrando arquivos de outubro e novembro]
*Figura 6 – Volume com os arquivos dos dois lotes.*

### Requisito 05 · Bronze incremental e sem duplicação
O Auto Loader (`cloudFiles`, `trigger(availableNow=True)`) guarda no checkpoint quais arquivos já leu. No lote 2 só
entraram os arquivos novos, e rodar de novo sem arquivos novos não inseriu nenhuma linha. As contagens de cada
execução ficam em `controle_cargas_bronze` (detalhadas na seção 7).

[PRINT: resultado da consulta de controle_cargas_bronze no notebook 01]
*Figura 7 – Contagens da Bronze antes e depois de cada execução.*

### Requisito 06 · Prata: tratamento, MERGE e regras de qualidade
A Prata converte os tipos (`TIMESTAMP`, `DOUBLE`, `INT`) com `try_cast`/`try_to_timestamp`, normaliza tipo, status e
bairro (ex.: "Queda de energia" → `queda_energia`), aceita datas em dois formatos e preenche nulos (domicílios recebem a
mediana do tipo; canal vira `nao_informado`). Antes do `MERGE` pela chave `id_chamado`, os registros são deduplicados, e
o MERGE só atualiza um chamado quando o registro novo é mais recente: é assim que as mudanças de status do lote 2 entram.

| Regra | Barra quando | Barrados |
|---|---|---|
| R1_obrigatorios | id, tipo ou data de abertura nulo ou ilegível | [n] |
| R2_dominio | tipo ou status fora dos valores aceitos | [n] |
| R3_localizacao | bairro fora de Osasco ou GPS fora do município | [n] |
| R4_datas_coerentes | resolução anterior à abertura | [n] |

[PRINT: resultado de "Total barrado por regra" no notebook 02]
*Figura 8 – Total barrado por cada regra de qualidade.*

[PRINT: histórico do MERGE mostrando linhas inseridas e atualizadas no lote 2]
*Figura 9 – MERGE da Prata: no lote 2, [n] chamados foram atualizados (mudança de status) e [n] inseridos.*

### Requisito 07 · Ouro dimensional e 5 consultas
A Ouro é um modelo estrela com a fato `ouro_fato_chamados` (1 linha por chamado) e cinco dimensões: tempo, clima,
bairro (com dados do IBGE), tipo de problema e cidadão (pseudonimizado). As cinco consultas estão na seção 8.

[PRINT: diagrama do modelo estrela (README no GitHub) ou a lista de tabelas ouro_* no Catalog]
*Figura 10 – Modelo estrela da camada Ouro.*

### Requisito 08 · LGPD na Ouro e uso pelo MVP
Na Ouro o CPF vira `sha2(salt + cpf, 256)`, com um salt secreto digitado no notebook (fora do repositório). Nome e
telefone ficam mascarados, e-mail e número da casa não entram e as coordenadas são arredondadas (~100 m). O MVP lê
**somente** a tabela `ouro_mvp_painel_chamados`, que é o contrato com o produto (a tela está na seção 9).

[PRINT: consulta "antes × depois" do notebook 03 (Prata × Ouro), com dados fictícios]
*Figura 11 – Dados pessoais na Prata (acesso restrito) e protegidos na Ouro (hash e máscara).*

### Requisito 09 · Repositório e README
O repositório público tem `dags/`, `databricks/`, `amostras/`, `docs/` e o `README.md` com integrantes, arquitetura e
o passo a passo para reproduzir. Não há chaves, senhas ou dados pessoais reais: `chegada/`, `.env` e certificados locais
estão no `.gitignore`, e as amostras usam dados fictícios.

[PRINT: página inicial do repositório no GitHub]
*Figura 12 – Repositório público com o README.*

## 6. Prints do Airflow

[PRINT: visão Graph do DAG akona_coleta_diaria]
*Figura 13 – Visão Graph: inicio → coletar_clima ∥ coletar_ibge → simular_chamados → conferir_chegada → notificar.*

[PRINT: visão Grid com as execuções concluídas (verdes) desde 01/10]
*Figura 14 – Visão Grid: [n] execuções diárias concluídas com sucesso.*

## 7. Contagens da Bronze antes e depois do 2º lote e da 2ª execução

| Execução | Fonte | Linhas antes | Linhas depois | Linhas novas | Arquivos depois |
|---|---|---|---|---|---|
| lote_1 | chamados | 0 | [n] | [n] | [n] |
| lote_2 | chamados | [n] | [n] | [n] | [n] |
| reexecucao | chamados | [n] | [n] | **0** | [n] |
| lote_1 | clima | 0 | [n] | [n] | [n] |
| lote_2 | clima | [n] | [n] | [n] | [n] |
| reexecucao | clima | [n] | [n] | **0** | [n] |
| lote_1 | ibge | 0 | [n] | [n] | [n] |
| lote_2 | ibge | [n] | [n] | [n] | [n] |
| reexecucao | ibge | [n] | [n] | **0** | [n] |

O lote 2 acrescentou só os arquivos de novembro, e a 2ª execução sem arquivos novos manteve as contagens:
**a Bronze é incremental e não duplica**.

[PRINT: a mesma tabela no notebook 01]
*Figura 15 – Log controle_cargas_bronze com as três execuções.*

## 8. As 5 consultas SQL, com resultado e interpretação

O SQL completo está em `databricks/04_consultas_mvp.sql`.

### Consulta 1 · Quais bairros têm mais chamados abertos, e de que tipo?
```sql
SELECT b.bairro, COUNT(*) AS chamados_abertos,
       COUNT_IF(t.tipo_problema = 'fio_caido') AS fio_caido,
       COUNT_IF(t.tipo_problema = 'queda_energia') AS queda_energia,
       COUNT_IF(t.tipo_problema = 'poste_apagado') AS poste_apagado,
       COUNT_IF(t.tipo_problema = 'lampada_piscando') AS lampada_piscando,
       COUNT_IF(f.sla_vencido) AS com_sla_vencido
FROM workspace.akona.ouro_fato_chamados f
JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
WHERE f.status <> 'resolvido'
GROUP BY b.bairro
ORDER BY chamados_abertos DESC, com_sla_vencido DESC
LIMIT 10;
```
[PRINT: resultado]
*Figura 16 – Bairros com mais chamados abertos, por tipo.*

**Interpretação:** [bairro 1] lidera com [n] chamados abertos, a maioria de [tipo], que é responsabilidade da
[prefeitura/distribuidora]. [n] deles já passaram do SLA. Os bairros com mais backlog também são os de atendimento
mais lento (ver consulta 2), então é ali que a gestão deve concentrar equipes.

### Consulta 2 · Qual o tempo médio de resolução por tipo de problema e por bairro?
```sql
WITH resolvidos AS (
  SELECT t.tipo_descricao AS tipo_problema, b.bairro, f.tempo_resolucao_horas, f.sla_vencido, t.sla_horas
  FROM workspace.akona.ouro_fato_chamados f
  JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
  JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
  WHERE f.status = 'resolvido'
),
agregado AS (
  SELECT tipo_problema,
         COALESCE(bairro, '(todos os bairros)') AS bairro_exibido,
         GROUPING(bairro) AS eh_total,
         COUNT(*) AS resolvidos,
         ROUND(AVG(tempo_resolucao_horas), 1) AS media_horas,
         ROUND(PERCENTILE(tempo_resolucao_horas, 0.5), 1) AS mediana_horas,
         MAX(sla_horas) AS sla_horas,
         ROUND(100 * AVG(CASE WHEN sla_vencido THEN 0 ELSE 1 END), 1) AS pct_dentro_sla
  FROM resolvidos
  GROUP BY GROUPING SETS ((tipo_problema), (tipo_problema, bairro))
)
SELECT tipo_problema, bairro_exibido AS bairro, resolvidos, media_horas, mediana_horas, sla_horas, pct_dentro_sla
FROM agregado
QUALIFY eh_total = 1 OR ROW_NUMBER() OVER (PARTITION BY tipo_problema, eh_total ORDER BY media_horas DESC) <= 3
ORDER BY tipo_problema, eh_total DESC, media_horas DESC;
```
[PRINT: resultado]
*Figura 17 – Tempo de resolução por tipo e os bairros mais lentos.*

**Interpretação:** fio caído é resolvido em média em [x] h (SLA 8 h) e queda de energia em [x] h (SLA 24 h). Já poste
apagado leva [x] h e lâmpada piscando [x] h, com só [x]% dentro do SLA. [Bairro] é o mais lento em [tipo], com média [x]
vezes maior que a média do tipo.

### Consulta 3 · Quais chamados devem ser priorizados?
```sql
SELECT ROW_NUMBER() OVER (ORDER BY f.prioridade_score DESC, f.aberto_em) AS posicao,
       f.id_chamado, t.tipo_descricao AS tipo_problema, b.bairro, f.status,
       f.horas_em_aberto, t.sla_horas, f.sla_vencido, f.moradores_afetados_estimados, f.prioridade_score
FROM workspace.akona.ouro_fato_chamados f
JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
WHERE f.status <> 'resolvido'
ORDER BY posicao
LIMIT 15;
```
[PRINT: resultado]
*Figura 18 – Fila de prioridade (top 15).*

**Interpretação:** o score soma o peso do tipo (fio caído 100, queda 80, poste 40, lâmpada 20), até 30 pontos pela
espera, 20 se o SLA venceu e até 20 pelo número de moradores afetados. Por isso o topo da fila é de [fio caído/queda de
energia], que oferecem risco ou deixam mais gente sem energia. O primeiro poste apagado aparece na posição [n], porque
está aberto há [x] h com o SLA vencido.

### Consulta 4 · Chuva e vento aumentam os chamados de queda de energia?
```sql
WITH por_dia AS (
  SELECT c.data, c.condicao_clima, c.precipitacao_mm, c.rajada_max_kmh,
         COUNT_IF(t.tipo_problema = 'queda_energia') AS quedas_energia,
         COUNT_IF(t.categoria = 'Rede elétrica') AS chamados_rede_eletrica,
         COUNT_IF(t.categoria = 'Iluminação pública') AS chamados_iluminacao
  FROM workspace.akona.ouro_dim_clima c
  LEFT JOIN workspace.akona.ouro_fato_chamados f ON f.data_key = c.data_key
  LEFT JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
  GROUP BY c.data, c.condicao_clima, c.precipitacao_mm, c.rajada_max_kmh
)
SELECT condicao_clima, COUNT(*) AS dias,
       ROUND(AVG(precipitacao_mm), 1) AS chuva_media_mm,
       ROUND(AVG(rajada_max_kmh), 1) AS rajada_media_kmh,
       ROUND(AVG(quedas_energia), 2) AS quedas_energia_por_dia,
       ROUND(AVG(chamados_rede_eletrica), 2) AS rede_eletrica_por_dia,
       ROUND(AVG(chamados_iluminacao), 2) AS iluminacao_por_dia,
       (SELECT ROUND(corr(quedas_energia, precipitacao_mm), 2) FROM por_dia) AS correlacao_chuva_quedas,
       (SELECT ROUND(corr(quedas_energia, rajada_max_kmh), 2) FROM por_dia) AS correlacao_rajada_quedas
FROM por_dia
GROUP BY condicao_clima
ORDER BY quedas_energia_por_dia DESC;
```
[PRINT: resultado]
*Figura 19 – Chamados por dia conforme o clima.*

**Interpretação:** em dias de **temporal** (chuva de 15 mm ou mais, ou rajada de 50 km/h ou mais) houve em média [x]
quedas de energia por dia, contra [x] em dias secos, ou seja, [x] vezes mais. A correlação entre chuva e quedas é de [r].
Os chamados de iluminação pública quase não mudam com o clima (controle). **Resposta: sim.** *Observação:* os chamados
são simulados e o simulador usa o clima real do dia como fator de risco para a rede elétrica. A consulta mostra que o
pipeline captura essa relação; com os chamados reais do webapp, ela mediria o efeito verdadeiro.

### Consulta 5 (impacto) · Quantos moradores são afetados por problemas não resolvidos?
```sql
-- moradores afetados = domicílios afetados × média de moradores por domicílio (IBGE, Censo 2022)
WITH impacto AS (
  SELECT COALESCE(t.tipo_descricao, 'TOTAL') AS tipo_problema,
         COUNT(*) AS chamados_nao_resolvidos,
         SUM(f.domicilios_afetados) AS domicilios_afetados,
         SUM(f.moradores_afetados_estimados) AS moradores_afetados,
         ROUND(100.0 * SUM(f.moradores_afetados_estimados) / MAX(b.populacao_municipio), 2) AS pct_populacao_osasco,
         COUNT_IF(f.sla_vencido) AS com_sla_vencido
  FROM workspace.akona.ouro_fato_chamados f
  JOIN workspace.akona.ouro_dim_tipo_problema t ON t.tipo_key = f.tipo_key
  JOIN workspace.akona.ouro_dim_bairro b ON b.bairro_key = f.bairro_key
  WHERE f.status <> 'resolvido'
  GROUP BY ROLLUP (t.tipo_descricao)
)
SELECT * FROM impacto
ORDER BY tipo_problema = 'TOTAL', moradores_afetados DESC;
```
[PRINT: resultado]
*Figura 20 – Moradores afetados por problemas não resolvidos.*

**Interpretação:** hoje [n] moradores de Osasco ([x]% da população) estão afetados por [n] chamados não resolvidos.
A maior parte vem de [tipo], e [n] desses chamados já passaram do SLA. É o número que a gestão leva para a reunião de
prioridades. *Limitação:* é uma estimativa, e dois chamados sobre a mesma queda podem contar as mesmas casas.

## 9. Tela do MVP consumindo a tabela da Ouro

A tela do MVP é um painel AI/BI do Databricks com indicadores (chamados não resolvidos, moradores afetados, % com SLA
vencido, tempo médio de resolução), o gráfico de não resolvidos por bairro e tipo, a fila de prioridade com o
solicitante mascarado e o gráfico de chamados da rede elétrica × chuva por dia. Todos os dados vêm de **uma única
tabela da Ouro**, `workspace.akona.ouro_mvp_painel_chamados`.

[PRINT: o painel publicado]
*Figura 21 – Tela do MVP: painel da gestão do Akona Energy.*

[PRINT: aba Data do painel mostrando o SQL com FROM workspace.akona.ouro_mvp_painel_chamados]
*Figura 22 – Os datasets do painel leem somente a tabela-contrato da Ouro.*

## 10. Conclusão técnica e link do repositório

O pipeline cobre o ciclo completo pedido: coleta agendada no Airflow 3 com tarefas paralelas, conferência da chegada,
notificação e segredos em Variables; ingestão incremental e idempotente com Auto Loader; uma Prata que trata tipos e nulos,
barra registros inválidos com regras contadas e usa MERGE para incorporar as mudanças de status dos chamados; e uma Ouro
dimensional que responde às perguntas do MVP e protege os dados pessoais com hash e máscara.

Decisões importantes: (1) o simulador de chamados é determinístico e emite chamados novos e atualizações, o que torna
o MERGE realista e o reprocessamento seguro; (2) o MVP lê uma única tabela-contrato, o que isola o produto das mudanças
internas do modelo; (3) o salt do hash fica fora do repositório, porque um hash de CPF sem salt é reversível por força bruta.

Limitações e próximos passos: substituir o simulador pela API real do webapp; automatizar o envio ao Volume (fora do
Databricks Free Edition, com um conector ou Jobs); incluir os dados de interrupções da ANEEL para validar a consulta 4
com dados reais; e processar a Prata de forma incremental (streaming com `foreachBatch`) quando o volume crescer.

**Repositório:** [https://github.com/USUARIO/REPOSITORIO](https://github.com/USUARIO/REPOSITORIO)
