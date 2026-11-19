"""
Leitura e validação das configurações do projeto (pasta config/).

    config/projeto.yaml        configurações gerais (município, coordenadas, Databricks)
    config/fontes/<nome>.yaml  uma fonte de dados por arquivo (gera a DAG akona_<nome>)
    config/ouro.yaml           tabelas da camada Ouro e suas dependências
    config/consultas.yaml      notebooks de análise

Erros de cadastro aparecem como erro de importação da DAG no Airflow, com a mensagem abaixo.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

PASTA_CONFIG = Path(os.environ.get("AKONA_CONFIG", Path(__file__).resolve().parents[2] / "config"))
CAMPOS_FONTE = {"nome", "descricao", "extrator", "prata"}
EXTRATORES = {"http", "python"}


def _ler(caminho: Path) -> dict:
    return yaml.safe_load(Path(caminho).read_text(encoding="utf-8")) or {}


def projeto() -> dict:
    return _ler(PASTA_CONFIG / "projeto.yaml")


def databricks() -> dict:
    return projeto()["databricks"]


def _validar_fonte(arquivo: Path, cfg: dict) -> dict:
    faltando = CAMPOS_FONTE - cfg.keys()
    if faltando:
        raise ValueError(f"config/fontes/{arquivo.name}: faltam os campos {sorted(faltando)}")
    if cfg["nome"] != arquivo.stem:
        raise ValueError(f"config/fontes/{arquivo.name}: o campo nome deve ser igual ao nome do arquivo ('{arquivo.stem}')")
    if cfg["extrator"] not in EXTRATORES:
        raise ValueError(f"config/fontes/{arquivo.name}: extrator deve ser um de {sorted(EXTRATORES)}")
    if cfg["extrator"] == "http" and not cfg.get("requisicoes"):
        raise ValueError(f"config/fontes/{arquivo.name}: extrator http precisa da lista 'requisicoes'")
    if cfg["extrator"] == "python" and not cfg.get("funcao"):
        raise ValueError(f"config/fontes/{arquivo.name}: extrator python precisa do campo 'funcao' (em include/akona/apis.py)")
    if not cfg["prata"].get("notebook"):
        raise ValueError(f"config/fontes/{arquivo.name}: informe prata.notebook")
    cfg.setdefault("depende_de", [])
    cfg.setdefault("segredos", {})
    cfg.setdefault("bronze", {}).setdefault("formato", "json")
    return cfg


def fontes() -> dict[str, dict]:
    """Fontes ativas (ativo: false desliga uma fonte sem apagar o arquivo)."""
    resultado = {}
    for arquivo in sorted((PASTA_CONFIG / "fontes").glob("*.yaml")):
        cfg = _ler(arquivo)
        if cfg.get("ativo", True):
            resultado[cfg.get("nome", arquivo.stem)] = _validar_fonte(arquivo, cfg)
    for nome, cfg in resultado.items():
        for dependencia in cfg["depende_de"]:
            if dependencia not in resultado:
                raise ValueError(f"config/fontes/{nome}.yaml: depende_de cita '{dependencia}', que não existe ou está inativa")
    return resultado


def fonte(nome: str) -> dict:
    todas = fontes()
    if nome not in todas:
        raise ValueError(f"Fonte '{nome}' não encontrada em config/fontes/ (ou está com ativo: false)")
    return todas[nome]


def ouro() -> list[dict]:
    tabelas = _ler(PASTA_CONFIG / "ouro.yaml").get("tabelas", [])
    nomes = {t["nome"] for t in tabelas}
    for tabela in tabelas:
        tabela.setdefault("depende_de", [])
        for dependencia in tabela["depende_de"]:
            if dependencia not in nomes:
                raise ValueError(f"config/ouro.yaml: {tabela['nome']} depende de '{dependencia}', que não está na lista")
    return tabelas


def consultas() -> list[dict]:
    return _ler(PASTA_CONFIG / "consultas.yaml").get("notebooks", [])
