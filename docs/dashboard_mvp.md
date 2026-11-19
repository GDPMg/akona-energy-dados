# Tela do MVP: painel AI/BI no Databricks

A tela do MVP é um **dashboard AI/BI do Databricks** que lê **somente** a tabela-contrato da Ouro
`workspace.akona.ouro_mvp_painel_chamados`. É a visão da gestão (prefeitura e distribuidora): o que está aberto,
onde está, o que fazer primeiro e quantas pessoas estão sendo afetadas.

> Pré-requisito: o DAG `akona_lakehouse` já rodou pelo menos uma vez, então a tabela `ouro_mvp_painel_chamados` existe.

## 1. Criar o dashboard

1. Menu lateral **Dashboards** → **Create dashboard**.
2. Renomeie para **Akona Energy · Painel de chamados (Osasco)**.

## 2. Datasets (aba **Data** → **Create from SQL**)

Todos leem a mesma tabela da Ouro. É isso que comprova o contrato com o produto (requisito 08).

**`abertos`**: chamados não resolvidos
```sql
SELECT *, CAST(sla_vencido AS INT) AS sla_vencido_num
FROM workspace.akona.ouro_mvp_painel_chamados
WHERE status <> 'resolvido'
```

**`resolvidos`**: para o tempo médio de resolução
```sql
SELECT *
FROM workspace.akona.ouro_mvp_painel_chamados
WHERE status = 'resolvido'
```

**`por_dia`**: chamados por dia × chuva
```sql
SELECT data_abertura,
       condicao_clima,
       MAX(chuva_no_dia_mm)                         AS chuva_mm,
       COUNT_IF(categoria = 'Rede elétrica')        AS chamados_rede_eletrica,
       COUNT_IF(categoria = 'Iluminação pública')   AS chamados_iluminacao
FROM workspace.akona.ouro_mvp_painel_chamados
GROUP BY data_abertura, condicao_clima
```

## 3. Widgets (aba do canvas)

| # | Widget | Dataset | Configuração |
|---|---|---|---|
| 1 | Texto | — | Título "Akona Energy · Painel de chamados (Osasco)" e a linha "Fonte: workspace.akona.ouro_mvp_painel_chamados (camada Ouro) · atualização diária" |
| 2 | Counter **Chamados não resolvidos** | `abertos` | Value: `COUNT(id_chamado)` |
| 3 | Counter **Moradores afetados (estimativa)** | `abertos` | Value: `SUM(moradores_afetados_estimados)` |
| 4 | Counter **% com SLA vencido** | `abertos` | Value: `AVG(sla_vencido_num)`, formato percentual |
| 5 | Counter **Tempo médio de resolução (h)** | `resolvidos` | Value: `AVG(tempo_resolucao_horas)` |
| 6 | Bar **Não resolvidos por bairro e tipo** | `abertos` | X: `bairro` · Y: `COUNT(id_chamado)` · Color: `tipo_problema` · empilhado, ordenado do maior para o menor |
| 7 | Table **Fila de prioridade** | `abertos` | Colunas: `posicao_fila`, `id_chamado`, `tipo_problema`, `bairro`, `horas_em_aberto`, `sla_vencido`, `moradores_afetados_estimados`, `solicitante`, `contato` · ordenar por `posicao_fila` crescente |
| 8 | Combo **Rede elétrica × chuva por dia** | `por_dia` | X: `data_abertura` · barras: `SUM(chamados_rede_eletrica)` · linha (eixo secundário): `SUM(chuva_mm)`. Se não houver combo, use um gráfico de linhas com as duas séries |
| 9 | Filtros | `abertos`, `resolvidos` | Filtro de **bairro** (campo `bairro`) e de **tipo** (campo `tipo_problema`) |

Sugestão de layout: título no topo, os 4 counters em linha, depois barras (esquerda) + combo (direita) e a fila de
prioridade ocupando a largura toda embaixo.

## 4. Publicar e tirar os prints

1. **Publish** (canto superior direito).
2. Print 1: o painel publicado inteiro, legível (zoom do navegador em 90% ou 100%).
3. Print 2: a aba **Data** mostrando o SQL de um dataset com `FROM workspace.akona.ouro_mvp_painel_chamados`.
   Esse print prova "qual tabela da Ouro o MVP usa".
4. (Opcional) **⋮ → Export** para salvar o arquivo `.lvdash.json` em `databricks/dashboard/` no repositório.

## 5. Contrato de dados: o que o MVP consome e mostra

| Coluna de `ouro_mvp_painel_chamados` | Onde aparece na tela |
|---|---|
| `status` | separa abertos e resolvidos (datasets) |
| `bairro`, `tipo_problema` | gráfico de barras, filtros e fila |
| `moradores_afetados_estimados` | counter "Moradores afetados" e fila |
| `sla_vencido` | counter "% com SLA vencido" e fila |
| `tempo_resolucao_horas` | counter "Tempo médio de resolução" |
| `posicao_fila`, `prioridade_score`, `horas_em_aberto` | tabela "Fila de prioridade" |
| `solicitante`, `contato` | fila (sempre **mascarados**: `M**** S****`, `(11) *****-1234`) |
| `data_abertura`, `categoria`, `chuva_no_dia_mm`, `condicao_clima` | gráfico "Rede elétrica × chuva por dia" |

O painel **não** tem acesso a CPF, nome completo, telefone, e-mail ou número da casa: essas colunas não existem na Ouro.
