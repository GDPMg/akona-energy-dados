"""
Simulador da API de chamados do Akona Energy: para um dia D, devolve os chamados abertos em D e as
atualizações de status de chamados anteriores.

- Determinístico: cada chamado sai do próprio ID (AK-AAAAMMDD-TT-NNN), então o mesmo dia gera o mesmo arquivo.
- O clima real do dia (chegada/clima/) aumenta fio caído e queda de energia.
- Dados pessoais fictícios: CPF com dígito verificador inválido e e-mails @example.com.
- Parte dos registros sai suja de propósito para exercitar as regras da Prata.

Uso local: python simulador_chamados.py 2026-10-01 2026-10-31 --chegada ./chegada --token teste-local
"""
from __future__ import annotations

import json
import math
import random
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path


@dataclass(frozen=True)
class TipoChamado:
    nome: str
    taxa_base_dia: float      # média de chamados novos por dia (sem efeito do clima)
    sensivel_clima: bool      # chuva e vento multiplicam a taxa?
    horas_inicio: tuple       # (mín, máx) horas entre abertura e início do atendimento
    horas_reparo: tuple       # (mín, máx) horas entre início e resolução
    domicilios: tuple         # (mín, máx) domicílios afetados
    prob_sem_solucao: float   # chance de o chamado ficar parado (backlog)


TIPOS = {
    "FC": TipoChamado("fio_caido", 0.7, True, (0.3, 2), (1, 8), (5, 40), 0.01),
    "QE": TipoChamado("queda_energia", 1.5, True, (0.5, 4), (1, 20), (20, 600), 0.02),
    "PA": TipoChamado("poste_apagado", 6.0, False, (12, 96), (2, 72), (4, 25), 0.10),
    "LP": TipoChamado("lampada_piscando", 3.5, False, (24, 168), (2, 96), (2, 12), 0.15),
}

# bairro: (peso na quantidade de chamados, fator de lentidão do atendimento)
BAIRROS = {
    "Centro": (1.2, 0.8),
    "Vila Yara": (0.8, 0.7),
    "Presidente Altino": (1.0, 0.9),
    "Bela Vista": (0.9, 1.0),
    "Jaguaribe": (0.8, 1.0),
    "Km 18": (1.0, 1.1),
    "Quitaúna": (0.9, 1.2),
    "Rochdale": (1.4, 1.5),
    "Novo Osasco": (1.2, 1.3),
    "Pestana": (0.7, 1.0),
    "Bonfim": (0.8, 1.1),
    "Vila Campesina": (0.7, 0.9),
    "Jardim das Flores": (1.3, 1.6),
    "Munhoz Júnior": (1.1, 1.4),
    "Santo Antônio": (1.2, 1.5),
    "Ayrosa": (0.9, 1.0),
}

LOGRADOUROS = [
    "Rua das Palmeiras", "Rua dos Ipês", "Rua São João", "Avenida Brasil",
    "Rua Sete de Setembro", "Rua Tiradentes", "Rua Santa Rita", "Rua das Acácias",
    "Rua Dom Pedro II", "Rua XV de Novembro", "Rua das Flores", "Avenida Central",
    "Rua do Comércio", "Travessa da Paz", "Rua das Hortênsias", "Rua Monte Alegre",
]

DESCRICOES = {
    "fio_caido": [
        "Fio caído na calçada, risco de choque",
        "Fio rompido pendurado depois da chuva",
        "Galho derrubou fio na rua",
        "Fio solto encostando no muro",
    ],
    "queda_energia": [
        "Rua inteira sem energia",
        "Falta de luz desde a chuva",
        "Quarteirão sem energia elétrica",
        "Estouro no transformador e queda de energia",
    ],
    "poste_apagado": [
        "Poste apagado em frente ao número {n}",
        "Rua escura, poste sem luz há dias",
        "Poste da esquina não acende à noite",
        "Iluminação apagada perto do ponto de ônibus",
    ],
    "lampada_piscando": [
        "Lâmpada do poste piscando a noite toda",
        "Luz do poste acende e apaga",
        "Lâmpada fraca e piscando na esquina",
    ],
}

NOMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique",
    "Isabela", "João", "Juliana", "Lucas", "Mariana", "Natália", "Otávio", "Patrícia",
    "Rafael", "Sabrina", "Thiago", "Vanessa", "Wesley", "Yasmin", "Leonardo", "Camila",
    "Diego", "Fernanda", "Gustavo", "Larissa", "Pedro", "Renata",
]
SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira",
    "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes",
    "Soares", "Fernandes", "Vieira", "Barbosa", "Rocha", "Dias", "Nascimento", "Andrade",
    "Moreira", "Nunes", "Marques", "Machado", "Mendes", "Freitas",
]
CANAIS = ["app", "web", "telefone"]
PESOS_CANAIS = [0.60, 0.25, 0.15]

# Sujeira proposital para a Prata tratar ou barrar
VARIANTES_TIPO = {
    "poste_apagado": ["Poste apagado", "POSTE_APAGADO", " poste apagado "],
    "lampada_piscando": ["Lâmpada piscando", "LAMPADA_PISCANDO", "lampada piscando"],
    "fio_caido": ["Fio caído", "FIO_CAIDO", "fio caido"],
    "queda_energia": ["Queda de energia", "QUEDA_ENERGIA", "queda de energia "],
}
VARIANTES_STATUS = {
    "aberto": ["Aberto", "ABERTO"],
    "em_andamento": ["Em andamento", "EM_ANDAMENTO"],
    "resolvido": ["Resolvido", "RESOLVIDO "],
}
TIPOS_INVALIDOS = ["buraco_na_rua", "outro", "barulho"]
BAIRROS_FORA_DE_OSASCO = ["Pinheiros", "Alphaville", "Não sei"]

FORMATO_DATA = "%Y-%m-%d %H:%M:%S"


@dataclass
class Chamado:
    id_chamado: str
    aberto_em: datetime
    inicio_em: datetime
    resolvido_em: datetime | None   # None = ficou parado (backlog)
    dados: dict                     # campos que não mudam ao longo da vida do chamado


def _sem_acento(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")


def _poisson(rng: random.Random, media: float) -> int:
    """Sorteia um número de eventos com média `media` (algoritmo de Knuth)."""
    limite, k, p = math.exp(-media), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limite:
            return k
        k += 1


def _digito_cpf(digitos: list[int], peso_inicial: int) -> int:
    soma = sum(d * p for d, p in zip(digitos, range(peso_inicial, 1, -1)))
    return (soma * 10) % 11 % 10


def _cpf_ficticio(rng: random.Random) -> str:
    """CPF com formato correto, mas com o 1º dígito verificador ERRADO de propósito.
    Assim ele nunca coincide com o CPF de uma pessoa real."""
    base = [rng.randint(0, 9) for _ in range(9)]
    d1_correto = _digito_cpf(base, 10)
    d1_errado = (d1_correto + rng.randint(1, 9)) % 10
    d2 = _digito_cpf(base + [d1_errado], 11)
    n = "".join(map(str, base)) + f"{d1_errado}{d2}"
    return f"{n[:3]}.{n[3:6]}.{n[6:9]}-{n[9:]}"


def _gravar_atomico(destino: Path, conteudo: str) -> None:
    """Grava em .tmp e renomeia: quem lê a pasta nunca vê arquivo pela metade."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(conteudo, encoding="utf-8")
    temporario.replace(destino)


def _para_json(valor):
    if isinstance(valor, datetime):
        return valor.strftime(FORMATO_DATA)
    raise TypeError(f"tipo não serializável: {type(valor)}")


def fator_clima(arquivo_clima: Path | None) -> tuple[float, float, float]:
    """Lê o arquivo do Open-Meteo do dia e devolve (fator, chuva_mm, rajada_max_kmh).
    Dia seco e sem vento forte: fator 1. Temporal: fator 3 a 5."""
    if not arquivo_clima or not Path(arquivo_clima).exists():
        return 1.0, 0.0, 0.0
    horario = json.loads(Path(arquivo_clima).read_text(encoding="utf-8")).get("hourly", {})
    chuva = sum(v or 0 for v in horario.get("precipitation", []))
    rajada = max((v or 0 for v in horario.get("wind_gusts_10m", [])), default=0)
    fator = 1 + min(chuva, 50) / 12 + max(0.0, rajada - 40) / 10
    return round(fator, 2), round(chuva, 1), round(rajada, 1)


def montar_chamado(id_chamado: str) -> Chamado:
    """Reconstrói o chamado inteiro (dados + ciclo de vida) a partir do ID."""
    _, data_txt, codigo, _ = id_chamado.split("-")
    dia = datetime.strptime(data_txt, "%Y%m%d").date()
    tipo = TIPOS[codigo]
    rng = random.Random(id_chamado)

    bairro = rng.choices(list(BAIRROS), weights=[peso for peso, _ in BAIRROS.values()])[0]
    lentidao = BAIRROS[bairro][1]
    aberto_em = datetime.combine(dia, time()) + timedelta(minutes=rng.randint(0, 24 * 60 - 1))
    inicio_em = aberto_em + timedelta(hours=rng.uniform(*tipo.horas_inicio) * lentidao)
    resolvido_em = None
    if rng.random() >= tipo.prob_sem_solucao:
        resolvido_em = inicio_em + timedelta(hours=rng.uniform(*tipo.horas_reparo) * lentidao)

    nome = rng.choice(NOMES)
    sobrenome1, sobrenome2 = rng.choice(SOBRENOMES), rng.choice(SOBRENOMES)
    numero = rng.randint(1, 2500)
    dados = {
        "id_chamado": id_chamado,
        "tipo_problema": tipo.nome,
        "descricao": rng.choice(DESCRICOES[tipo.nome]).format(n=numero),
        "bairro": bairro,
        "logradouro": rng.choice(LOGRADOUROS),
        "numero": str(numero),
        "latitude": round(rng.uniform(-23.555, -23.495), 6),
        "longitude": round(rng.uniform(-46.825, -46.755), 6),
        "domicilios_afetados": rng.randint(*tipo.domicilios),
        "canal": rng.choices(CANAIS, weights=PESOS_CANAIS)[0],
        "cidadao_nome": f"{nome} {sobrenome1} {sobrenome2}",
        "cidadao_cpf": _cpf_ficticio(rng),
        "cidadao_telefone": f"(11) 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}",
        "cidadao_email": _sem_acento(f"{nome}.{sobrenome1}{rng.randint(1, 99)}@example.com").lower(),
    }
    return Chamado(id_chamado, aberto_em, inicio_em, resolvido_em, dados)


def registro_no_fim_do_dia(chamado: Chamado, dia: date) -> dict:
    """Estado do chamado às 23:59:59 do dia (o que a API devolveria naquele dia)."""
    fim_do_dia = datetime.combine(dia, time(23, 59, 59))
    if chamado.resolvido_em and chamado.resolvido_em <= fim_do_dia:
        status, atualizado_em = "resolvido", chamado.resolvido_em
    elif chamado.inicio_em <= fim_do_dia:
        status, atualizado_em = "em_andamento", chamado.inicio_em
    else:
        status, atualizado_em = "aberto", chamado.aberto_em

    d = chamado.dados
    return {
        "id_chamado": d["id_chamado"],
        "tipo_problema": d["tipo_problema"],
        "status": status,
        "descricao": d["descricao"],
        "bairro": d["bairro"],
        "logradouro": d["logradouro"],
        "numero": d["numero"],
        "latitude": d["latitude"],
        "longitude": d["longitude"],
        "domicilios_afetados": d["domicilios_afetados"],
        "canal": d["canal"],
        "aberto_em": chamado.aberto_em,
        "atualizado_em": atualizado_em,
        "resolvido_em": chamado.resolvido_em if status == "resolvido" else None,
        "cidadao_nome": d["cidadao_nome"],
        "cidadao_cpf": d["cidadao_cpf"],
        "cidadao_telefone": d["cidadao_telefone"],
        "cidadao_email": d["cidadao_email"],
    }


def _teve_evento_no_dia(chamado: Chamado, dia: date) -> bool:
    return chamado.inicio_em.date() == dia or (
        chamado.resolvido_em is not None and chamado.resolvido_em.date() == dia
    )


def _ids_novos(dia: date, fator: float) -> list[str]:
    rng = random.Random(f"akona-{dia.isoformat()}")
    ids = []
    for codigo, tipo in TIPOS.items():
        media = tipo.taxa_base_dia * (fator if tipo.sensivel_clima else 1.0)
        for i in range(1, _poisson(rng, media) + 1):
            ids.append(f"AK-{dia:%Y%m%d}-{codigo}-{i:03d}")
    return ids


def _ids_anteriores(dia: date, pasta_chegada: Path, inicio: date, janela_dias: int = 60) -> set[str]:
    """IDs dos chamados abertos nos dias anteriores, lidos dos arquivos já gerados."""
    ids: set[str] = set()
    for k in range(1, janela_dias + 1):
        dia_anterior = dia - timedelta(days=k)
        if dia_anterior < inicio:
            break
        arquivo = pasta_chegada / "chamados" / f"chamados_{dia_anterior.isoformat()}.json"
        if not arquivo.exists():
            continue
        prefixo = f"AK-{dia_anterior:%Y%m%d}-"
        for linha in arquivo.read_text(encoding="utf-8").splitlines():
            if linha.strip():
                id_chamado = json.loads(linha).get("id_chamado") or ""
                if id_chamado.startswith(prefixo):
                    ids.add(id_chamado)
    return ids


def _sujar(registro: dict, rng: random.Random) -> list[dict]:
    """Aplica problemas de qualidade a uma fração pequena dos registros."""
    r = dict(registro)

    # 1) erros que a Prata deve BARRAR (no máximo um por registro)
    sorteio = rng.random()
    if sorteio < 0.010:
        r["tipo_problema"] = None                                      # R1: obrigatório nulo
    elif sorteio < 0.020:
        r["aberto_em"] = "31/02/2026 25:61"                            # R1: data ilegível
    elif sorteio < 0.035:
        r["tipo_problema"] = rng.choice(TIPOS_INVALIDOS)               # R2: fora do domínio
    elif sorteio < 0.050:
        r["latitude"], r["longitude"] = 0.0, 0.0                       # R3: GPS zerado
    elif sorteio < 0.058:
        r["bairro"] = rng.choice(BAIRROS_FORA_DE_OSASCO)               # R3: fora de Osasco
    elif sorteio < 0.075 and r["resolvido_em"] is not None:
        r["resolvido_em"] = r["aberto_em"] - timedelta(hours=rng.randint(2, 48))  # R4

    # 2) nulos que a Prata deve TRATAR (preencher)
    if rng.random() < 0.05:
        r["domicilios_afetados"] = None
    if rng.random() < 0.03:
        r["canal"] = None

    # 3) variações de formato que a Prata deve NORMALIZAR
    if r["tipo_problema"] in VARIANTES_TIPO and rng.random() < 0.30:
        r["tipo_problema"] = rng.choice(VARIANTES_TIPO[r["tipo_problema"]])
    if r["bairro"] in BAIRROS and rng.random() < 0.20:
        r["bairro"] = rng.choice([r["bairro"].upper(), f"  {r['bairro']} ", _sem_acento(r["bairro"])])
    if rng.random() < 0.20:
        r["status"] = rng.choice(VARIANTES_STATUS[r["status"]])
    if isinstance(r["aberto_em"], datetime) and rng.random() < 0.10:
        r["aberto_em"] = r["aberto_em"].strftime("%d/%m/%Y %H:%M")
    if rng.random() < 0.30:
        r["cidadao_cpf"] = r["cidadao_cpf"].replace(".", "").replace("-", "")

    # 4) linha duplicada (a Prata deve deduplicar antes do MERGE)
    return [r, dict(r)] if rng.random() < 0.02 else [r]


def _validar_token(token: str | None) -> None:
    if not token or token.strip().upper().startswith("TROQUE"):
        raise PermissionError(
            "Token da API Akona ausente ou de exemplo. Crie a Variable 'akona_api_token' no Airflow."
        )


def gerar_conteudo_chamados(dia: str, pasta_chegada: Path, token: str,
                            inicio_simulacao: str = "2026-10-01") -> tuple[str, dict]:
    """Resposta da API simulada para o dia: conteúdo JSON Lines (um chamado por linha) e um resumo."""
    _validar_token(token)
    data = date.fromisoformat(dia)
    pasta_chegada = Path(pasta_chegada)

    fator, chuva, rajada = fator_clima(pasta_chegada / "clima" / f"clima_{dia}.json")
    novos = _ids_novos(data, fator)
    anteriores = _ids_anteriores(data, pasta_chegada, date.fromisoformat(inicio_simulacao))

    registros = [registro_no_fim_do_dia(montar_chamado(i), data) for i in novos]
    atualizacoes = 0
    for id_chamado in sorted(anteriores):
        chamado = montar_chamado(id_chamado)
        if _teve_evento_no_dia(chamado, data):
            registros.append(registro_no_fim_do_dia(chamado, data))
            atualizacoes += 1

    linhas = []
    for registro in registros:
        rng = random.Random(f"{registro['id_chamado']}|{dia}|sujeira")
        for r in _sujar(registro, rng):
            linhas.append(json.dumps(r, ensure_ascii=False, default=_para_json))

    resumo = {
        "dia": dia, "novos": len(novos), "atualizacoes": atualizacoes, "linhas": len(linhas),
        "fator_clima": fator, "chuva_mm": chuva, "rajada_max_kmh": rajada,
    }
    return "\n".join(linhas) + "\n", resumo


def gerar_arquivo_chamados(dia: str, pasta_chegada: Path, token: str,
                           inicio_simulacao: str = "2026-10-01") -> tuple[Path, dict]:
    """Gera chegada/chamados/chamados_<dia>.json (usado nos testes locais pela linha de comando)."""
    conteudo, resumo = gerar_conteudo_chamados(dia, pasta_chegada, token, inicio_simulacao)
    destino = Path(pasta_chegada) / "chamados" / f"chamados_{dia}.json"
    _gravar_atomico(destino, conteudo)
    return destino, resumo


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gera arquivos de chamados simulados do Akona Energy.")
    parser.add_argument("inicio", help="primeiro dia (AAAA-MM-DD)")
    parser.add_argument("fim", nargs="?", help="último dia (AAAA-MM-DD); padrão = inicio")
    parser.add_argument("--chegada", default="chegada", help="pasta de pouso (padrão: ./chegada)")
    parser.add_argument("--token", default="token-local-de-teste", help="token fictício para testes locais")
    args = parser.parse_args()

    atual = date.fromisoformat(args.inicio)
    fim = date.fromisoformat(args.fim or args.inicio)
    while atual <= fim:
        _, resumo = gerar_arquivo_chamados(atual.isoformat(), Path(args.chegada), args.token,
                                           inicio_simulacao=args.inicio)
        print(resumo)
        atual += timedelta(days=1)
