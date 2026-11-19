# Como adicionar uma fonte nova

O pipeline é **orientado a configuração**: cada fonte de dados é um cadastro em `config/fontes/`, e o código
genérico monta o resto. Exemplo: trazer o **PIB municipal de Osasco** do IBGE (agregado 5938) como uma tabela nova.

## Visão geral: o que você cria e o que vem pronto

| Você cria | Vem pronto (não precisa mexer) |
|---|---|
| `config/fontes/ibge_pib.yaml`: URL, parâmetros, dependências | chamada da API, gravação em `chegada/`, conferência dos arquivos |
| `dags/akona_ibge_pib.py`: 2 linhas | DAG completa: API → chegada → Volume → Bronze → Prata |
| `databricks/prata/prata_ibge_pib.py`: copiado do modelo | Bronze (notebook genérico), regras de qualidade, MERGE, logs |
| (opcional) `databricks/ouro/<tabela>.sql` + 1 item em `config/ouro.yaml` | publicação dos notebooks e execução no Databricks |

A `trigger-master` descobre a fonte nova sozinha, a partir de `config/fontes/`, e passa a dispará-la na ordem certa.

## Passo 1 · Cadastro da fonte: `config/fontes/ibge_pib.yaml`

```yaml
nome: ibge_pib                       # igual ao nome do arquivo; vira akona_ibge_pib, chegada/ibge_pib/, bronze_ibge_pib
descricao: IBGE - PIB dos municípios
extrator: http                       # http = chamada genérica (include/akona/apis.py), sem programar
depende_de: []                       # fontes que precisam ser coletadas antes desta

requisicoes:                         # 1 requisição = arquivo é a resposta bruta; várias = envelope JSON
  - nome: pib
    url: https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/-1/variaveis/37
    parametros:
      localidades: "N6[{cod_ibge}]"  # {cod_ibge}, {latitude}, {dia}... vêm de config/projeto.yaml

bronze:
  formato: json

prata:
  notebook: prata/prata_ibge_pib     # notebook em databricks/ (sem extensão)
  tabela: prata_ibge_pib
```

Campos que podem aparecer:

| Campo | Para quê |
|---|---|
| `extrator: http` + `requisicoes` | API REST comum: `url`, `parametros`, `cabecalhos`, `timeout` |
| `envelope: true` | grava as várias respostas num JSON só, uma por campo (como `ibge.yaml`) |
| `extrator: python` + `funcao` | API com lógica própria: crie a função em `include/akona/apis.py` (ex.: `chamar_api_akona`) |
| `segredos` | ex.: `token: minha_variable`. O valor vem da **Airflow Variable** e pode ser usado como `{token}` na URL ou nos cabeçalhos. Nunca coloque chaves no YAML |
| `depende_de` | ex.: `[clima]`. A trigger-master só dispara esta fonte depois das dependências |
| `ativo: false` | desliga a fonte sem apagar o arquivo |

**Toda chamada de API fica em `include/akona/apis.py`.** Só mexa nele se a API precisar de lógica que o extrator
`http` não cobre.

## Passo 2 · DAG da fonte: `dags/akona_ibge_pib.py`

```python
"""DAG do Airflow da fonte IBGE_PIB: API -> chegada -> Volume -> Bronze -> Prata (config/fontes/ibge_pib.yaml)."""
from akona.dag_fonte import criar_dag_fonte

dag = criar_dag_fonte("ibge_pib")
```

Todas as DAGs de fonte seguem o mesmo padrão (`include/akona/dag_fonte.py`):
`coletar_api → conferir_chegada → enviar_ao_volume → bronze → prata`, com `preparar_databricks` e
`publicar_notebooks` em paralelo.

## Passo 3 · Prata: `databricks/prata/prata_ibge_pib.py`

1. Copie `databricks/prata/_modelo_prata.py`.
2. Troque `FONTE`, `TABELA` e `CHAVES`.
3. Converta os campos da Bronze (tudo texto) para os tipos certos.
4. Escreva as regras de qualidade.

Normalização, contagem das regras, deduplicação e MERGE vêm de `databricks/comum/funcoes.py`, por `%run`.

> Atalho para o IBGE: a resposta da API de agregados tem sempre o mesmo formato. O `prata/prata_ibge.py` já sabe
> achatá-la (variável → resultados → séries → `{ano: valor}`). Para um agregado novo no **mesmo** envelope da fonte
> `ibge`, basta incluir o nome do campo na lista `AGREGADOS` desse notebook.

## Passo 4 (opcional) · Levar para a Ouro

1. Crie `databricks/ouro/<tabela>.sql` com `CREATE OR REPLACE TABLE workspace.akona.ouro_<tabela> ... AS SELECT ...`.
2. Acrescente em `config/ouro.yaml`:
   ```yaml
   - nome: <tabela>
     depende_de: [dim_tempo]          # tabelas da Ouro que precisam existir antes
   ```
3. A DAG `akona_ouro` cria a tarefa e respeita a ordem.

Para uma consulta de análise nova, crie `databricks/consultas/<nome>.sql` e acrescente o item em
`config/consultas.yaml`.

## Passo 5 · Testar

1. Abra o Airflow: a DAG `akona_ibge_pib` aparece em até 2 minutos. Se algo estiver errado no YAML, a mensagem aparece em
   **Dag Import Errors**.
2. Rode a DAG sozinha (Trigger, com `modo` = `full` para trazer todo o histórico) ou a `trigger-master` inteira.
3. Confira no Databricks: `bronze_ibge_pib`, `prata_ibge_pib` e as linhas em `controle_qualidade`.
