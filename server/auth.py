import os, secrets
from fastapi import Header, HTTPException
from typing import Optional

API_KEY      = os.getenv("BACKUP_API_KEY", "")
AUTH_ENABLED = bool(API_KEY)


def require_api_key(x_api_key: Optional[str] = Header(None)):
    if not AUTH_ENABLED:
        return
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(401, "API key invalida")
