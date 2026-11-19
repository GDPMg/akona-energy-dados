"""
Chamadas de API do projeto Akona Energy: TODA comunicação com fontes de dados fica neste arquivo.

As fontes são descritas em config/fontes/<nome>.yaml e usam um de dois extratores:

- extrator: http   -> chamada genérica (coletar_http). URL e parâmetros vêm do YAML; nada a programar.
                       1 requisição  = o arquivo do dia é a resposta bruta da API.
                       N requisições = o arquivo do dia é um "envelope" JSON com a resposta de cada uma.
- extrator: python -> uma função deste arquivo, indicada em `funcao:` no YAML. Use quando a API exige
                       lógica própria (ex.: chamar_api_akona). A função recebe (fonte, dia, contexto) e
                       devolve o conteúdo do arquivo do dia (texto).

Nos textos do YAML, {dia} é o dia coletado; {latitude}, {cod_ibge} etc. vêm de config/projeto.yaml e
{token} etc. vêm de Airflow Variables (campo `segredos:` do YAML).
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from akona.simulador_chamados import gerar_conteudo_chamados

TIMEOUT_PADRAO = 30


def preencher(valor, contexto: dict):
    """Troca {campo} pelos valores do contexto, também dentro de listas e dicionários."""
    if isinstance(valor, str):
        return valor.format_map(contexto)
    if isinstance(valor, dict):
        return {chave: preencher(v, contexto) for chave, v in valor.items()}
    if isinstance(valor, list):
        return [preencher(v, contexto) for v in valor]
    return valor


def chamar_api(requisicao: dict, contexto: dict) -> requests.Response:
    """GET de uma requisição descrita no YAML (url, parametros, cabecalhos, timeout)."""
    resposta = requests.get(
        preencher(requisicao["url"], contexto),
        params=preencher(requisicao.get("parametros", {}), contexto),
        headers=preencher(requisicao.get("cabecalhos", {}), contexto),
        timeout=requisicao.get("timeout", TIMEOUT_PADRAO),
    )
    resposta.raise_for_status()
    return resposta


def coletar_http(fonte: dict, dia: str, contexto: dict) -> str:
    """Extrator genérico: executa as requisições da fonte e devolve o conteúdo do arquivo do dia."""
    respostas = {req["nome"]: chamar_api(req, contexto) for req in fonte["requisicoes"]}
    if len(respostas) == 1 and not fonte.get("envelope", False):
        return next(iter(respostas.values())).text                      # resposta bruta
    pacote = {
        "fonte": fonte["descricao"],
        "dia_coleta": dia,
        "coletado_em": datetime.now(ZoneInfo(contexto["timezone"])).isoformat(),
    }
    pacote.update({nome: resposta.json() for nome, resposta in respostas.items()})
    return json.dumps(pacote, ensure_ascii=False)


def chamar_api_akona(fonte: dict, dia: str, contexto: dict) -> str:
    """API de chamados do webapp Akona. Enquanto o webapp não existe, a resposta vem do simulador
    (include/akona/simulador_chamados.py), que exige o token como a API real exigiria."""
    conteudo, _ = gerar_conteudo_chamados(
        dia, Path(contexto["pasta_chegada"]), token=contexto.get("token"),
        inicio_simulacao=contexto["inicio_simulacao"])
    return conteudo


def extrair(fonte: dict, dia: str, contexto: dict) -> str:
    """Ponto único de entrada: escolhe o extrator da fonte."""
    if fonte["extrator"] == "http":
        return coletar_http(fonte, dia, contexto)
    funcao = globals().get(fonte["funcao"])
    if not callable(funcao):
        raise ValueError(f"Fonte {fonte['nome']}: função '{fonte['funcao']}' não existe em include/akona/apis.py")
    return funcao(fonte, dia, contexto)
