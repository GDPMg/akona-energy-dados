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
    def _chamar(self, metodo: str, caminho: str, aceitar=(200,), **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", 60)
        resposta = self.sessao.request(metodo, f"{self.host}{caminho}", **kwargs)
        if resposta.status_code not in aceitar:
            raise ErroDatabricks(f"{metodo} {caminho} -> HTTP {resposta.status_code}: {resposta.text[:800]}")
        return resposta

    def _json(self, metodo: str, caminho: str, **kwargs) -> dict:
        resposta = self._chamar(metodo, caminho, **kwargs)
        return resposta.json() if resposta.content else {}

    # ------------------------------------------------------------------ Unity Catalog
    def garantir_schema(self, catalogo: str, schema: str, comentario: str = "") -> str:
        r = self._chamar("GET", f"/api/2.1/unity-catalog/schemas/{catalogo}.{schema}", aceitar=(200, 404))
        if r.status_code == 200:
            return "já existia"
        self._json("POST", "/api/2.1/unity-catalog/schemas",
                   json={"catalog_name": catalogo, "name": schema, "comment": comentario})
        return "criado"

    def garantir_volume(self, catalogo: str, schema: str, volume: str, comentario: str = "") -> str:
        r = self._chamar("GET", f"/api/2.1/unity-catalog/volumes/{catalogo}.{schema}.{volume}", aceitar=(200, 404))
        if r.status_code == 200:
            return "já existia"
        self._json("POST", "/api/2.1/unity-catalog/volumes",
                   json={"catalog_name": catalogo, "schema_name": schema, "name": volume,
                         "volume_type": "MANAGED", "comment": comentario})
        return "criado"

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
        escopos = {s["name"] for s in self._json("GET", "/api/2.0/secrets/scopes/list").get("scopes", [])}
        if escopo not in escopos:
            r = self._chamar("POST", "/api/2.0/secrets/scopes/create", aceitar=(200, 400), json={"scope": escopo})
            if r.status_code == 400:  # alguns workspaces exigem initial_manage_principal
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
