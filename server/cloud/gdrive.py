"""Google Drive CloudProvider implementation.

Requer variáveis de ambiente:
  GDRIVE_CLIENT_ID     — OAuth2 Client ID (Google Cloud Console)
  GDRIVE_CLIENT_SECRET — OAuth2 Client Secret

Escopos solicitados: drive.readonly + userinfo.email + userinfo.profile
"""
import os, hashlib, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode

import httpx

from .base import CloudProvider, FileEntry

log = logging.getLogger("backup-server")

_CLIENT_ID     = os.getenv("GDRIVE_CLIENT_ID", "")
_CLIENT_SECRET = os.getenv("GDRIVE_CLIENT_SECRET", "")
_SCOPES        = " ".join([
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
])
_AUTH_URI  = "https://accounts.google.com/o/oauth2/auth"
_TOKEN_URI = "https://oauth2.googleapis.com/token"


class GoogleDriveProvider(CloudProvider):
    provider_name = "gdrive"

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        params = {
            "client_id":     _CLIENT_ID,
            "redirect_uri":  redirect_uri,
            "response_type": "code",
            "scope":         _SCOPES,
            "access_type":   "offline",
            "prompt":        "consent",
            "state":         state,
        }
        return f"{_AUTH_URI}?{urlencode(params)}"

    async def exchange_code(self, code: str, redirect_uri: str) -> dict:
        async with httpx.AsyncClient() as client:
            r = await client.post(_TOKEN_URI, data={
                "code":          code,
                "client_id":     _CLIENT_ID,
                "client_secret": _CLIENT_SECRET,
                "redirect_uri":  redirect_uri,
                "grant_type":    "authorization_code",
            })
            r.raise_for_status()
            data = r.json()
        return {
            "access_token":  data["access_token"],
            "refresh_token": data.get("refresh_token"),
            "expiry": datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600)),
        }

    async def refresh_tokens(self, refresh_token: str) -> dict:
        async with httpx.AsyncClient() as client:
            r = await client.post(_TOKEN_URI, data={
                "refresh_token": refresh_token,
                "client_id":     _CLIENT_ID,
                "client_secret": _CLIENT_SECRET,
                "grant_type":    "refresh_token",
            })
            r.raise_for_status()
            data = r.json()
        return {
            "access_token": data["access_token"],
            "expiry": datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600)),
        }

    async def get_account_info(self, access_token: str) -> dict:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://www.googleapis.com/oauth2/v1/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            r.raise_for_status()
            data = r.json()
        return {"email": data["email"], "display_name": data.get("name", data["email"])}

    async def list_root_folders(self, access_token: str) -> list[FileEntry]:
        return await self.list_folder(access_token, "root")

    async def list_folder(self, access_token: str, folder_id: str) -> list[FileEntry]:
        results: list[FileEntry] = []
        page_token: str | None = None
        async with httpx.AsyncClient() as client:
            while True:
                params: dict = {
                    "q":        f"'{folder_id}' in parents and trashed=false",
                    "fields":   "nextPageToken,files(id,name,mimeType,size,modifiedTime)",
                    "pageSize": "1000",
                }
                if page_token:
                    params["pageToken"] = page_token
                r = await client.get(
                    "https://www.googleapis.com/drive/v3/files",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params=params,
                )
                if r.is_error:
                    log.error(f"[gdrive] list_folder {folder_id} → HTTP {r.status_code}: {r.text}")
                r.raise_for_status()
                data = r.json()
                for f in data.get("files", []):
                    is_folder = f["mimeType"] == "application/vnd.google-apps.folder"
                    try:
                        mtime = datetime.fromisoformat(
                            f["modifiedTime"].replace("Z", "+00:00")
                        ).timestamp()
                    except Exception:
                        mtime = 0.0
                    results.append(FileEntry(
                        file_id=f["id"],
                        name=f["name"],
                        path=f["name"],
                        size=int(f.get("size", 0)) if not is_folder else 0,
                        mtime=mtime,
                        is_folder=is_folder,
                    ))
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
        return results

    async def download_file_to(
        self, access_token: str, file_id: str, dest_path: Path, chunk_size: int = 1024 * 1024
    ) -> tuple[str, int]:
        h = hashlib.sha256()
        size = 0
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
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
