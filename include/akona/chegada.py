"""
Pasta de pouso (chegada/): padrão de nomes, gravação e conferência dos arquivos coletados.

Padrão: chegada/<fonte>/<fonte>_AAAA-MM-DD.json  (um arquivo por fonte e por dia de coleta)
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta
from pathlib import Path

PASTA_CHEGADA = Path(os.environ.get("AKONA_CHEGADA", "/opt/airflow/chegada"))
_PADRAO_NOME = re.compile(r"^(?P<fonte>[a-z0-9_]+)_(?P<dia>\d{4}-\d{2}-\d{2})\.json$")


def arquivo_do_dia(fonte: str, dia: str) -> Path:
    return PASTA_CHEGADA / fonte / f"{fonte}_{dia}.json"


def dias_do_periodo(de: str, ate: str) -> list[str]:
    inicio, fim = date.fromisoformat(de), date.fromisoformat(ate)
    if inicio > fim:
        raise ValueError(f"Período inválido: de={de} é depois de ate={ate}")
    return [(inicio + timedelta(days=i)).isoformat() for i in range((fim - inicio).days + 1)]


def gravar(destino: Path, conteudo: str) -> Path:
    """Grava em .tmp e renomeia: quem lê a pasta nunca vê arquivo pela metade."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(conteudo, encoding="utf-8")
    temporario.replace(destino)
    return destino


def conferir(arquivo: Path) -> int:
    """Confere se o arquivo existe, não está vazio e se cada linha é JSON válido. Devolve o nº de registros."""
    if not arquivo.exists() or arquivo.stat().st_size == 0:
        raise FileNotFoundError(f"{arquivo.name}: não chegou ou está vazio")
    linhas = [linha for linha in arquivo.read_text(encoding="utf-8").splitlines() if linha.strip()]
    for numero, linha in enumerate(linhas, start=1):
        try:
            json.loads(linha)
        except json.JSONDecodeError as erro:
            raise ValueError(f"{arquivo.name}: linha {numero} não é JSON válido ({erro})") from erro
    return len(linhas)


def arquivos_do_periodo(fonte: str, de: str, ate: str) -> list[Path]:
    """Arquivos da fonte que existem localmente com data entre de e ate."""
    pasta = PASTA_CHEGADA / fonte
    encontrados = []
    for arquivo in sorted(pasta.glob(f"{fonte}_*.json")):
        achado = _PADRAO_NOME.match(arquivo.name)
        if achado and achado["fonte"] == fonte and de <= achado["dia"] <= ate:
            encontrados.append(arquivo)
    return encontrados
