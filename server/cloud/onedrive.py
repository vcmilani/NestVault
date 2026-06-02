"""OneDrive CloudProvider implementation via Microsoft Graph API.

Requer variáveis de ambiente:
  ONEDRIVE_CLIENT_ID     — Application (client) ID (Azure Portal → App registrations)
  ONEDRIVE_CLIENT_SECRET — Client Secret

Escopos: Files.Read offline_access openid profile email
Tenant:  common (aceita contas pessoais e corporativas)
"""
import os, hashlib, logging, secrets, base64
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode

import httpx

from .base import CloudProvider, FileEntry, TokenRevokedError

log = logging.getLogger("backup-server")

_CLIENT_ID     = os.getenv("ONEDRIVE_CLIENT_ID", "")
_CLIENT_SECRET = os.getenv("ONEDRIVE_CLIENT_SECRET", "")
_AUTHORITY     = "https://login.microsoftonline.com/consumers/oauth2/v2.0"
_SCOPES        = "https://graph.microsoft.com/Files.Read https://graph.microsoft.com/User.Read offline_access openid profile email"
_GRAPH_BASE    = "https://graph.microsoft.com/v1.0"


def generate_pkce() -> tuple[str, str]:
    """Retorna (code_verifier, code_challenge) para PKCE S256."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


class OneDriveProvider(CloudProvider):
    provider_name = "onedrive"

    def get_auth_url(self, redirect_uri: str, state: str, code_challenge: str) -> str:
        params = {
            "client_id":             _CLIENT_ID,
            "response_type":         "code",
            "redirect_uri":          redirect_uri,
            "scope":                 _SCOPES,
            "response_mode":         "query",
            "state":                 state,
            "code_challenge":        code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{_AUTHORITY}/authorize?{urlencode(params)}"

    async def exchange_code(self, code: str, redirect_uri: str, code_verifier: str) -> dict:
        log.info(
            f"[onedrive] exchange_code → client_id={'OK' if _CLIENT_ID else 'VAZIO'} "
            f"secret={'OK' if _CLIENT_SECRET else 'VAZIO'} "
            f"redirect_uri={redirect_uri!r} "
            f"code_len={len(code)} verifier_len={len(code_verifier)}"
        )
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{_AUTHORITY}/token", data={
                "client_id":     _CLIENT_ID,
                "code":          code,
                "redirect_uri":  redirect_uri,
                "grant_type":    "authorization_code",
                "scope":         _SCOPES,
                "code_verifier": code_verifier,
            })
            if not r.is_success:
                body = r.text
                log.error(f"[onedrive] exchange_code falhou {r.status_code}: {body}")
                raise httpx.HTTPStatusError(
                    f"{r.status_code} {r.reason_phrase} — {body}",
                    request=r.request, response=r,
                )
            data = r.json()
        return {
            "access_token":  data["access_token"],
            "refresh_token": data.get("refresh_token"),
            "expiry": datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600)),
        }

    async def refresh_tokens(self, refresh_token: str) -> dict:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{_AUTHORITY}/token", data={
                "client_id":     _CLIENT_ID,
                "refresh_token": refresh_token,
                "grant_type":    "refresh_token",
                "scope":         _SCOPES,
            })
            if not r.is_success:
                body = r.text
                log.error(f"[onedrive] refresh_tokens falhou {r.status_code}: {body}")
                if r.status_code == 400:
                    try:
                        err = r.json().get("error", "")
                    except Exception:
                        err = ""
                    if err in ("invalid_grant", "interaction_required"):
                        raise TokenRevokedError("Refresh token inválido ou expirado — re-autenticação necessária")
                raise httpx.HTTPStatusError(
                    f"{r.status_code} {r.reason_phrase} — {body}",
                    request=r.request, response=r,
                )
            data = r.json()
        return {
            "access_token": data["access_token"],
            "expiry": datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600)),
        }

    async def get_account_info(self, access_token: str) -> dict:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{_GRAPH_BASE}/me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if not r.is_success:
                log.error(f"[onedrive] get_account_info falhou {r.status_code}: {r.text}")
            r.raise_for_status()
            data = r.json()
        email = data.get("mail") or data.get("userPrincipalName", "")
        name  = data.get("displayName", email)
        return {"email": email, "display_name": name}

    async def list_root_folders(self, access_token: str) -> list[FileEntry]:
        return await self._list_children(access_token, "/me/drive/root/children")

    async def list_folder(self, access_token: str, folder_id: str) -> list[FileEntry]:
        return await self._list_children(access_token, f"/me/drive/items/{folder_id}/children")

    async def list_folder_recursive(
        self, access_token: str, folder_id: str, prefix: str = ""
    ) -> list[FileEntry]:
        # Cria um único cliente com timeout generoso e reutiliza em toda a árvore,
        # evitando reconectar a cada subpasta em árvores grandes.
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=60.0)) as client:
            return await self._recurse(client, access_token, folder_id, prefix)

    async def _recurse(
        self, client: httpx.AsyncClient, access_token: str, folder_id: str, prefix: str
    ) -> list[FileEntry]:
        entries = await self._list_children(
            access_token, f"/me/drive/items/{folder_id}/children", client=client
        )
        result: list[FileEntry] = []
        for e in entries:
            e.path = f"{prefix}/{e.name}".lstrip("/") if prefix else e.name
            if e.is_folder:
                result.extend(await self._recurse(client, access_token, e.file_id, e.path))
            else:
                result.append(e)
        return result

    async def _list_children(
        self, access_token: str, path: str, *, client: httpx.AsyncClient | None = None
    ) -> list[FileEntry]:
        results: list[FileEntry] = []
        url: str | None = f"{_GRAPH_BASE}{path}?$select=id,name,folder,file,size,lastModifiedDateTime&$top=1000"
        owned = client is None
        if owned:
            client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=60.0))
        try:
            while url:
                r = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
                r.raise_for_status()
                data = r.json()
                for item in data.get("value", []):
                    is_folder = "folder" in item
                    try:
                        mtime = datetime.fromisoformat(
                            item["lastModifiedDateTime"].replace("Z", "+00:00")
                        ).timestamp()
                    except Exception:
                        mtime = 0.0
                    results.append(FileEntry(
                        file_id=item["id"],
                        name=item["name"],
                        path=item["name"],
                        size=item.get("size", 0) if not is_folder else 0,
                        mtime=mtime,
                        is_folder=is_folder,
                    ))
                url = data.get("@odata.nextLink")
        finally:
            if owned:
                await client.aclose()
        return results

    async def download_file_to(
        self, access_token: str, file_id: str, dest_path: Path, chunk_size: int = 1024 * 1024
    ) -> tuple[str, int]:
        h = hashlib.sha256()
        size = 0
        # Graph retorna 302 para a URL de download direto
        url = f"{_GRAPH_BASE}/me/drive/items/{file_id}/content"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=300.0)) as client:
            async with client.stream(
                "GET", url,
                headers={"Authorization": f"Bearer {access_token}"},
                follow_redirects=True,
            ) as r:
                r.raise_for_status()
                with open(dest_path, "wb", buffering=0) as f:
                    async for chunk in r.aiter_bytes(chunk_size=chunk_size):
                        h.update(chunk)
                        f.write(chunk)
                        size += len(chunk)
        return h.hexdigest(), size
