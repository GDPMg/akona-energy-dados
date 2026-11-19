"""
Todas as chamadas de API das fontes de dados (config/fontes/<nome>.yaml).

- extrator: http   -> chamada genérica; URL e parâmetros vêm do YAML.
- extrator: python -> função deste arquivo indicada em `funcao:`; recebe (fonte, dia, contexto) e devolve o texto do arquivo.

Placeholders: {dia}; campos de config/projeto.yaml; {token} e outros `segredos:` vêm de Airflow Variables.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from akona.simulador_chamados import gerar_conteudo_chamados
from akona.simulador_manutencao import gerar_conteudo_os

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
        return next(iter(respostas.values())).text
    pacote = {
        "fonte": fonte["descricao"],
        "dia_coleta": dia,
        "coletado_em": datetime.now(ZoneInfo(contexto["timezone"])).isoformat(),
    }
    pacote.update({nome: resposta.json() for nome, resposta in respostas.items()})
    return json.dumps(pacote, ensure_ascii=False)


def chamar_api_akona(fonte: dict, dia: str, contexto: dict) -> str:
    """API de chamados do webapp Akona (simulada em simulador_chamados.py; exige token)."""
    conteudo, _ = gerar_conteudo_chamados(
        dia, Path(contexto["pasta_chegada"]), token=contexto.get("token"),
        inicio_simulacao=contexto["inicio_simulacao"])
    return conteudo


def chamar_api_manutencao(fonte: dict, dia: str, contexto: dict) -> str:
    """API de manutenção da iluminação pública (simulada em simulador_manutencao.py; exige token)."""
    if not contexto.get("token") or str(contexto["token"]).upper().startswith("TROQUE"):
        raise PermissionError("Token de integração ausente. Crie a Variable 'akona_api_token' no Airflow.")
    conteudo, _ = gerar_conteudo_os(dia, Path(contexto["pasta_chegada"]))
    return conteudo


def extrair(fonte: dict, dia: str, contexto: dict) -> str:
    """Ponto único de entrada: escolhe o extrator da fonte."""
    if fonte["extrator"] == "http":
        return coletar_http(fonte, dia, contexto)
    funcao = globals().get(fonte["funcao"])
    if not callable(funcao):
        raise ValueError(f"Fonte {fonte['nome']}: função '{fonte['funcao']}' não existe em include/akona/apis.py")
    return funcao(fonte, dia, contexto)
