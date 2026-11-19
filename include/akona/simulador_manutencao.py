"""
Simulador da API do sistema de MANUTENÇÃO da iluminação pública (ordens de serviço) do Akona Energy.

Na operação real, quando um morador abre um chamado de iluminação (poste apagado ou lâmpada piscando), a prefeitura
abre uma ORDEM DE SERVIÇO (OS) para a equipe de manutenção, ligada ao PONTO DE ILUMINAÇÃO (o poste, ex.: OSC-03024).
Esse sistema ainda não existe; este módulo devolve, para um dia D, o que a API dele devolveria: as OS que tiveram
algum evento em D (abertura, início do atendimento ou conclusão), com o estado no fim do dia.

Como funciona
- Parte dos chamados do dia, já coletados em chegada/chamados/chamados_<D>.json (por isso depende da fonte chamados).
  O ciclo de vida da OS acompanha o do chamado (include/akona/simulador_chamados.py), então os tempos batem.
- Determinístico: o mesmo dia gera sempre o mesmo arquivo. Os sorteios usam sementes próprias e NÃO mudam
  nenhum dado dos chamados.
- Cada bairro tem POSTES_POR_BAIRRO pontos; o ponto de cada OS é sorteado a partir do ID do chamado. Com ~120 postes
  por bairro, cerca de 9% das OS caem num poste que já teve OS nos 30 dias anteriores (reincidência realista).
- Não há dado pessoal: as equipes são identificadas só por código (EQ-01 ... EQ-06).
- Uma pequena parte dos registros sai "suja" de propósito para exercitar as regras da Prata.
"""
from __future__ import annotations

import json
import random
from datetime import date, datetime, time
from pathlib import Path

from akona.simulador_chamados import BAIRROS, FORMATO_DATA, montar_chamado

TIPOS_DE_ILUMINACAO = {"PA": "poste_apagado", "LP": "lampada_piscando"}   # códigos no ID do chamado
CODIGO_BAIRRO = {bairro: i + 1 for i, bairro in enumerate(BAIRROS)}      # 01..16, usado no código do poste
POSTES_POR_BAIRRO = 120
EQUIPES = [f"EQ-{n:02d}" for n in range(1, 7)]
SERVICOS = {
    "poste_apagado": (["troca_lampada", "troca_reator", "troca_fotocelula", "reparo_fiacao"], [50, 25, 15, 10]),
    "lampada_piscando": (["troca_lampada", "troca_fotocelula", "troca_reator"], [60, 30, 10]),
}
VARIANTES_STATUS = {
    "aguardando_equipe": ["Aguardando equipe", "AGUARDANDO_EQUIPE"],
    "em_execucao": ["Em execução", "EM_EXECUCAO"],
    "concluida": ["Concluída", "CONCLUIDA"],
}


def ponto_do_chamado(id_chamado: str, bairro: str) -> dict:
    """Poste atendido: sorteado a partir do ID do chamado; atributos fixos por poste."""
    numero = random.Random(f"{id_chamado}|ponto").randint(1, POSTES_POR_BAIRRO)
    ponto_id = f"OSC-{CODIGO_BAIRRO[bairro]:02d}{numero:03d}"
    rng = random.Random(ponto_id)
    tecnologia = "led" if rng.random() < 0.7 else "vapor_de_sodio"
    potencia = rng.choice([60, 100, 150] if tecnologia == "led" else [70, 150, 250])
    return {"ponto_id": ponto_id, "tecnologia": tecnologia, "potencia_w": potencia}


def _ids_do_dia(dia: str, pasta_chegada: Path) -> list[str]:
    arquivo = Path(pasta_chegada) / "chamados" / f"chamados_{dia}.json"
    if not arquivo.exists():
        raise FileNotFoundError(
            f"{arquivo.name} não existe: a fonte manutencao depende da fonte chamados. Rode akona_chamados antes.")
    ids = []
    for linha in arquivo.read_text(encoding="utf-8").splitlines():
        if linha.strip():
            id_chamado = json.loads(linha).get("id_chamado") or ""       # AK-AAAAMMDD-TT-NNN
            partes = id_chamado.split("-")
            if len(partes) == 4 and partes[2] in TIPOS_DE_ILUMINACAO and id_chamado not in ids:
                ids.append(id_chamado)
    return ids


def ordem_no_fim_do_dia(id_chamado: str, dia: date) -> dict:
    """Estado da OS às 23:59:59 do dia (segue o ciclo de vida do chamado)."""
    chamado = montar_chamado(id_chamado)
    tipo, bairro = chamado.dados["tipo_problema"], chamado.dados["bairro"]
    fim_do_dia = datetime.combine(dia, time(23, 59, 59))
    rng = random.Random(f"{id_chamado}|os")
    servicos, pesos = SERVICOS[tipo]

    if chamado.resolvido_em and chamado.resolvido_em <= fim_do_dia:
        status, atualizado_em = "concluida", chamado.resolvido_em
    elif chamado.inicio_em <= fim_do_dia:
        status, atualizado_em = "em_execucao", chamado.inicio_em
    else:
        status, atualizado_em = "aguardando_equipe", chamado.aberto_em

    return {
        "os_id": "OS-" + id_chamado.removeprefix("AK-"),
        "id_chamado": id_chamado,
        **ponto_do_chamado(id_chamado, bairro),
        "bairro": bairro,
        "tipo_problema": tipo,
        "tipo_servico": rng.choices(servicos, weights=pesos)[0] if status == "concluida" else None,
        "equipe": rng.choice(EQUIPES) if status != "aguardando_equipe" else None,
        "status": status,
        "aberta_em": chamado.aberto_em,
        "iniciada_em": chamado.inicio_em if status != "aguardando_equipe" else None,
        "concluida_em": chamado.resolvido_em if status == "concluida" else None,
        "atualizado_em": atualizado_em,
    }


def _sujar(registro: dict, rng: random.Random) -> dict:
    r = dict(registro)
    sorteio = rng.random()
    if sorteio < 0.010:
        r["ponto_id"] = None                                         # M1: obrigatório nulo
    elif sorteio < 0.018:
        r["ponto_id"] = "POSTE-SEM-CODIGO"                           # M2: fora do padrão OSC-NNNNN
    elif sorteio < 0.024 and r["concluida_em"] is not None:
        r["concluida_em"] = r["aberta_em"].replace(year=r["aberta_em"].year - 1)   # M3: conclusão antes da abertura
    if rng.random() < 0.20:
        r["status"] = rng.choice(VARIANTES_STATUS[r["status"]])     # a Prata normaliza
    if r["bairro"] in BAIRROS and rng.random() < 0.15:
        r["bairro"] = r["bairro"].upper()
    return r


def gerar_conteudo_os(dia: str, pasta_chegada: Path) -> tuple[str, dict]:
    """Resposta da API simulada para o dia: JSON Lines (uma OS por linha) e um resumo."""
    data = date.fromisoformat(dia)
    linhas = []
    for id_chamado in _ids_do_dia(dia, pasta_chegada):
        registro = ordem_no_fim_do_dia(id_chamado, data)
        registro = _sujar(registro, random.Random(f"{id_chamado}|{dia}|sujeira_os"))
        linhas.append(json.dumps(registro, ensure_ascii=False,
                                 default=lambda v: v.strftime(FORMATO_DATA) if isinstance(v, datetime) else str(v)))
    return ("\n".join(linhas) + "\n") if linhas else "", {"dia": dia, "ordens": len(linhas)}
