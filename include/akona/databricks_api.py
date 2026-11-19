"""
Cliente mínimo da API REST do Databricks usado pelos DAGs do Akona Energy.

Só usa `requests`. Cobre o que o pipeline precisa:
- Unity Catalog: criar schema e Volume (se não existirem)
- Files API: criar pastas e enviar arquivos ao Volume (sem sobrescrever)
- Secrets: guardar o salt da LGPD (gerado aleatoriamente, ninguém vê o valor)
- Workspace: publicar os notebooks da pasta databricks/ do repositório
- Jobs: executar um notebook em computação serverless e esperar o resultado
"""
from __future__ import annotations

import base64
import logging
import secrets
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)
ESTADOS_FINAIS = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


class ErroDatabricks(RuntimeError):
    """Erro devolvido pela API do Databricks ou por uma execução de notebook."""


class Databricks:
    def __init__(self, host: str, token: str):
        self.host = host.rstrip("/")
        self.sessao = requests.Session()
        self.sessao.headers["Authorization"] = f"Bearer {token}"

    # ------------------------------------------------------------------ base
    TENTATIVAS = 5

    @staticmethod
    def _nao_conectou(erro: Exception) -> bool:
        """True quando a conexão nem chegou a ser aberta (nada foi enviado ao servidor)."""
        return isinstance(erro, requests.ConnectTimeout) or "Failed to establish a new connection" in str(erro)

    def _chamar(self, metodo: str, caminho: str, aceitar=(200,), **kwargs) -> requests.Response:
        """Chamada à API com novas tentativas em falhas passageiras de rede:
        - leituras (GET/HEAD) são repetidas em qualquer falha de conexão ou erro temporário do servidor;
        - envios (POST/PUT) só são repetidos quando a conexão nem abriu, para nunca duplicar uma ação."""
        kwargs.setdefault("timeout", 60)
        leitura = metodo in ("GET", "HEAD")
        for tentativa in range(1, self.TENTATIVAS + 1):
            try:
                resposta = self.sessao.request(metodo, f"{self.host}{caminho}", **kwargs)
                if leitura and resposta.status_code in (429, 500, 502, 503, 504) and tentativa < self.TENTATIVAS:
                    raise requests.ConnectionError(f"HTTP {resposta.status_code}")
                break
            except (requests.ConnectionError, requests.Timeout) as erro:
                if tentativa == self.TENTATIVAS or not (leitura or self._nao_conectou(erro)):
                    raise
                log.warning("Falha passageira em %s %s (%s); nova tentativa %d de %d",
                            metodo, caminho, erro, tentativa + 1, self.TENTATIVAS)
                time.sleep(2 * tentativa)
        if resposta.status_code not in aceitar:
            raise ErroDatabricks(f"{metodo} {caminho} -> HTTP {resposta.status_code}: {resposta.text[:800]}")
        return resposta

    def _json(self, metodo: str, caminho: str, **kwargs) -> dict:
        resposta = self._chamar(metodo, caminho, **kwargs)
        return resposta.json() if resposta.content else {}

    # ------------------------------------------------------------------ Unity Catalog
    def _criar_se_nao_existir(self, caminho_get: str, caminho_post: str, corpo: dict) -> str:
        """GET; se não existir, POST. Se outra DAG criou ao mesmo tempo (409/400 'already exists'), tudo bem."""
        if self._chamar("GET", caminho_get, aceitar=(200, 404)).status_code == 200:
            return "já existia"
        r = self._chamar("POST", caminho_post, aceitar=(200, 201, 400, 409), json=corpo)
        if r.status_code in (200, 201):
            return "criado"
        if self._chamar("GET", caminho_get, aceitar=(200, 404)).status_code == 200:
            return "já existia"
        raise ErroDatabricks(f"POST {caminho_post} -> HTTP {r.status_code}: {r.text[:800]}")

    def garantir_schema(self, catalogo: str, schema: str, comentario: str = "") -> str:
        return self._criar_se_nao_existir(
            f"/api/2.1/unity-catalog/schemas/{catalogo}.{schema}", "/api/2.1/unity-catalog/schemas",
            {"catalog_name": catalogo, "name": schema, "comment": comentario})

    def garantir_volume(self, catalogo: str, schema: str, volume: str, comentario: str = "") -> str:
        return self._criar_se_nao_existir(
            f"/api/2.1/unity-catalog/volumes/{catalogo}.{schema}.{volume}", "/api/2.1/unity-catalog/volumes",
            {"catalog_name": catalogo, "schema_name": schema, "name": volume,
             "volume_type": "MANAGED", "comment": comentario})

    # ------------------------------------------------------------------ Files API (Volumes)
    def criar_pasta(self, caminho: str) -> None:
        self._chamar("PUT", f"/api/2.0/fs/directories{caminho}", aceitar=(200, 201, 204))

    def arquivo_existe(self, caminho: str) -> bool:
        return self._chamar("HEAD", f"/api/2.0/fs/files{caminho}", aceitar=(200, 404)).status_code == 200

    def enviar_arquivo(self, local: Path, destino: str) -> None:
        self._chamar("PUT", f"/api/2.0/fs/files{destino}", aceitar=(200, 201, 204),
                     params={"overwrite": "false"}, data=Path(local).read_bytes(),
                     headers={"Content-Type": "application/octet-stream"}, timeout=120)

    # ------------------------------------------------------------------ Secrets
    def garantir_segredo(self, escopo: str, chave: str) -> str:
        """Cria o escopo e um segredo aleatório, se ainda não existirem. O valor nunca é exibido."""
        def escopos() -> set:
            return {s["name"] for s in self._json("GET", "/api/2.0/secrets/scopes/list").get("scopes", [])}

        if escopo not in escopos():
            r = self._chamar("POST", "/api/2.0/secrets/scopes/create", aceitar=(200, 400), json={"scope": escopo})
            if r.status_code == 400 and escopo not in escopos():  # alguns workspaces exigem initial_manage_principal
                self._json("POST", "/api/2.0/secrets/scopes/create",
                           json={"scope": escopo, "initial_manage_principal": "users"})
        chaves = {s["key"] for s in self._json("GET", "/api/2.0/secrets/list", params={"scope": escopo}).get("secrets", [])}
        if chave in chaves:
            return "já existia"
        self._json("POST", "/api/2.0/secrets/put",
                   json={"scope": escopo, "key": chave, "string_value": secrets.token_urlsafe(32)})
        return "criado"

    # ------------------------------------------------------------------ Workspace (notebooks)
    def publicar_notebook(self, local: Path, destino: str) -> None:
        local = Path(local)
        linguagem = "SQL" if local.suffix == ".sql" else "PYTHON"
        self._json("POST", "/api/2.0/workspace/mkdirs", json={"path": destino.rsplit("/", 1)[0]})
        self._json("POST", "/api/2.0/workspace/import", json={
            "path": destino, "format": "SOURCE", "language": linguagem, "overwrite": True,
            "content": base64.b64encode(local.read_bytes()).decode("ascii"),
        })

    # ------------------------------------------------------------------ Jobs (execução serverless)
    def rodar_notebook(self, caminho: str, parametros: dict | None = None, nome: str | None = None,
                       intervalo_s: int = 10, limite_s: int = 3600) -> dict:
        """Executa um notebook do workspace (serverless) e espera terminar. Falha se o notebook falhar."""
        nome = nome or caminho.rsplit("/", 1)[-1]
        corpo = {
            "run_name": f"akona - {nome}",
            "timeout_seconds": limite_s,
            "queue": {"enabled": True},
            "tasks": [{
                "task_key": nome,
                "notebook_task": {
                    "notebook_path": caminho,
                    "source": "WORKSPACE",
                    "base_parameters": {k: str(v) for k, v in (parametros or {}).items()},
                },
            }],
        }
        run_id = self._json("POST", "/api/2.2/jobs/runs/submit", json=corpo)["run_id"]
        inicio = time.monotonic()
        url = None
        while True:
            run = self._json("GET", "/api/2.2/jobs/runs/get", params={"run_id": run_id})
            url = run.get("run_page_url", url)
            estado = run.get("state", {})
            if estado.get("life_cycle_state") in ESTADOS_FINAIS:
                break
            if time.monotonic() - inicio > limite_s + 300:
                raise ErroDatabricks(f"Notebook {nome} passou do tempo limite. Veja: {url}")
            time.sleep(intervalo_s)

        segundos = round(time.monotonic() - inicio)
        if estado.get("result_state") != "SUCCESS":
            detalhe, rastro = estado.get("state_message", ""), ""
            for tarefa in run.get("tasks") or []:
                saida = self._json("GET", "/api/2.2/jobs/runs/get-output", params={"run_id": tarefa["run_id"]})
                detalhe = saida.get("error") or detalhe
                rastro = (saida.get("error_trace") or "")[-3000:]
            raise ErroDatabricks(
                f"Notebook {nome} terminou com {estado.get('result_state')}: {detalhe}\n{rastro}\nVeja: {url}")
        return {"notebook": nome, "run_id": run_id, "url": url, "segundos": segundos}
