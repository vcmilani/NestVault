"""FastAPI router para /cloud/* — contas e jobs de cloud backup."""
import asyncio, os, re, secrets, logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session, joinedload

from database import (
    get_db, CloudCredential, CloudBackupJob,
    encrypt_token, decrypt_token,
)
from auth import require_api_key

log = logging.getLogger("backup-server")

router = APIRouter(prefix="/cloud", tags=["cloud"])

# Estado OAuth em memória: state_token → {"redirect_uri", "provider"}
_pending_states: dict[str, dict] = {}

# Lock por job_id para impedir execuções paralelas do mesmo job
_job_locks: dict[int, asyncio.Lock] = {}

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")


# -- Helpers ------------------------------------------------------------------

def _get_provider(name: str):
    if name == "gdrive":
        from cloud.gdrive import GoogleDriveProvider
        return GoogleDriveProvider()
    if name == "onedrive":
        from cloud.onedrive import OneDriveProvider
        return OneDriveProvider()
    raise HTTPException(400, f"Provider desconhecido: '{name}'. Use 'gdrive' ou 'onedrive'.")


def _callback_uri(provider: str) -> str:
    return f"{BASE_URL}/cloud/callback/{provider}"


def _localhost_callback_uri(provider: str) -> str:
    """URI de redirect para fluxo manual — exibe o código ao usuário sem trocá-lo."""
    m = re.search(r':(\d+)$', BASE_URL.split('//')[-1])
    port = int(m.group(1)) if m else 8000
    return f"http://localhost:{port}/cloud/manual-redirect/{provider}"


def _require_credential(credential_id: int, db: Session) -> CloudCredential:
    c = db.get(CloudCredential, credential_id)
    if not c:
        raise HTTPException(404, f"Conta {credential_id} não encontrada")
    return c


def _require_job(job_id: int, db: Session) -> CloudBackupJob:
    j = db.get(CloudBackupJob, job_id)
    if not j:
        raise HTTPException(404, f"Job {job_id} não encontrado")
    return j


async def _fresh_token(credential: CloudCredential, db: Session) -> str:
    needs_refresh = (
        not credential.access_token
        or credential.token_expiry is None
        or credential.token_expiry.replace(tzinfo=timezone.utc) <= datetime.now(timezone.utc) + timedelta(minutes=5)
    )
    if needs_refresh:
        provider = _get_provider(credential.provider)
        tokens = await provider.refresh_tokens(decrypt_token(credential.refresh_token))
        credential.access_token = tokens["access_token"]
        credential.token_expiry = tokens.get("expiry")
        db.commit()
    return credential.access_token


# -- Schemas ------------------------------------------------------------------

class AccountOut(BaseModel):
    id: int
    provider: str
    email: str
    display_name: Optional[str]
    created_at: str

def _clean_folder_id(v: str) -> str:
    """Aceita URL completa do Drive ou só o ID; extrai só o ID."""
    if "/" in v:
        v = v.rstrip("/").split("/")[-1]
    return v.split("?")[0]

class JobCreate(BaseModel):
    credential_id: int
    folder_id: str
    folder_name: str
    target_label: str
    cron_expr: Optional[str] = None
    enabled: bool = True

    @field_validator("folder_id")
    @classmethod
    def clean_folder_id(cls, v): return _clean_folder_id(v)

class JobUpdate(BaseModel):
    folder_id: Optional[str] = None
    folder_name: Optional[str] = None
    target_label: Optional[str] = None
    cron_expr: Optional[str] = None
    enabled: Optional[bool] = None

    @field_validator("folder_id")
    @classmethod
    def clean_folder_id(cls, v): return _clean_folder_id(v) if v else v

class JobOut(BaseModel):
    id: int
    credential_id: int
    provider: str
    email: str
    folder_id: str
    folder_name: str
    target_label: str
    cron_expr: Optional[str]
    enabled: bool
    last_run_at: Optional[str]
    last_run_status: Optional[str]
    last_run_message: Optional[str]
    created_at: str

class FolderOut(BaseModel):
    id: str
    name: str
    is_folder: bool = True

class AuthUrlOut(BaseModel):
    url: str
    state: str
    redirect_uri: str

class ManualExchangeRequest(BaseModel):
    code: str
    state: str


# -- Contas -------------------------------------------------------------------

@router.get("/accounts", response_model=list[AccountOut], dependencies=[Depends(require_api_key)])
def list_accounts(db: Session = Depends(get_db)):
    return [
        AccountOut(
            id=c.id, provider=c.provider, email=c.email,
            display_name=c.display_name, created_at=str(c.created_at),
        )
        for c in db.query(CloudCredential).order_by(CloudCredential.created_at).all()
    ]


@router.get("/accounts/{provider}/auth", response_model=AuthUrlOut, dependencies=[Depends(require_api_key)])
def get_auth_url(provider: str, manual: bool = Query(False)):
    provider_obj = _get_provider(provider)
    state = secrets.token_urlsafe(24)
    redirect_uri = _localhost_callback_uri(provider) if manual else _callback_uri(provider)

    extra = {}
    if provider == "onedrive":
        from cloud.onedrive import generate_pkce
        verifier, challenge = generate_pkce()
        extra["code_verifier"] = verifier
        kwargs = {"code_challenge": challenge}
    else:
        kwargs = {}

    _pending_states[state] = {"provider": provider, "redirect_uri": redirect_uri, **extra}
    url = provider_obj.get_auth_url(redirect_uri=redirect_uri, state=state, **kwargs)
    return AuthUrlOut(url=url, state=state, redirect_uri=redirect_uri)


@router.post("/accounts/{provider}/exchange", response_model=AccountOut, status_code=201, dependencies=[Depends(require_api_key)])
async def manual_exchange(provider: str, req: ManualExchangeRequest, db: Session = Depends(get_db)):
    """Troca o código OAuth pelo token manualmente (fluxo para IPs privados)."""
    pending = _pending_states.pop(req.state, None)
    if not pending or pending["provider"] != provider:
        raise HTTPException(400, "State inválido ou expirado — reinicie o processo de autenticação")

    provider_obj = _get_provider(provider)
    exchange_kwargs = {"code": req.code, "redirect_uri": pending["redirect_uri"]}
    if "code_verifier" in pending:
        exchange_kwargs["code_verifier"] = pending["code_verifier"]
    tokens = await provider_obj.exchange_code(**exchange_kwargs)
    if not tokens.get("refresh_token"):
        raise HTTPException(400, "Provedor não retornou refresh_token. No Google, certifique-se de usar prompt=consent na primeira autorização.")

    info = await provider_obj.get_account_info(tokens["access_token"])
    cred = CloudCredential(
        provider=provider,
        email=info["email"],
        display_name=info.get("display_name"),
        access_token=tokens["access_token"],
        refresh_token=encrypt_token(tokens["refresh_token"]),
        token_expiry=tokens.get("expiry"),
    )
    db.add(cred)
    db.commit()
    db.refresh(cred)
    log.info(f"[cloud] Conta conectada (manual): {info['email']} ({provider})")
    return AccountOut(
        id=cred.id, provider=cred.provider, email=cred.email,
        display_name=cred.display_name, created_at=str(cred.created_at),
    )


@router.get("/manual-redirect/{provider}", include_in_schema=False)
async def manual_redirect_page(
    provider: str,
    code: str = Query(...),
    state: str = Query(...),
):
    """Página intermediária para o fluxo manual: exibe o código sem consumi-lo."""
    from fastapi.responses import HTMLResponse
    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="utf-8"><title>NestVault — Código de autorização</title>
<style>body{{font-family:sans-serif;max-width:480px;margin:60px auto;text-align:center}}
code{{background:#f3f4f6;padding:8px 14px;border-radius:6px;font-size:1.1em;word-break:break-all}}
p{{color:#555}}</style></head>
<body>
<h2>Autorização recebida</h2>
<p>Copie o código abaixo e cole no campo <strong>Código de autorização</strong> no NestVault:</p>
<code id="c">{code}</code><br><br>
<button onclick="navigator.clipboard.writeText(document.getElementById('c').textContent)">Copiar</button>
<p style="font-size:.85em;margin-top:24px">State: <code>{state}</code></p>
</body></html>"""
    return HTMLResponse(html)


@router.get("/callback/{provider}", include_in_schema=False)
async def oauth_callback(
    provider: str,
    code: str = Query(...),
    state: str = Query(...),
    db: Session = Depends(get_db),
):
    from fastapi.responses import RedirectResponse
    pending = _pending_states.pop(state, None)
    if not pending or pending["provider"] != provider:
        raise HTTPException(400, "State inválido ou expirado — reinicie o processo de autenticação")

    provider_obj = _get_provider(provider)
    redirect_uri = pending["redirect_uri"]

    exchange_kwargs = {"code": code, "redirect_uri": redirect_uri}
    if "code_verifier" in pending:
        exchange_kwargs["code_verifier"] = pending["code_verifier"]
    tokens = await provider_obj.exchange_code(**exchange_kwargs)
    if not tokens.get("refresh_token"):
        raise HTTPException(400, "Google/Microsoft não retornou refresh_token. Certifique-se de que prompt=consent está ativo.")

    info = await provider_obj.get_account_info(tokens["access_token"])

    cred = CloudCredential(
        provider=provider,
        email=info["email"],
        display_name=info.get("display_name"),
        access_token=tokens["access_token"],
        refresh_token=encrypt_token(tokens["refresh_token"]),
        token_expiry=tokens.get("expiry"),
    )
    db.add(cred)
    db.commit()
    log.info(f"[cloud] Conta conectada: {info['email']} ({provider})")
    return RedirectResponse(url="/?cloud_connected=1")


@router.delete("/accounts/{credential_id}", status_code=204, dependencies=[Depends(require_api_key)])
def disconnect_account(credential_id: int, db: Session = Depends(get_db)):
    c = _require_credential(credential_id, db)
    db.delete(c)
    db.commit()
    log.info(f"[cloud] Conta {credential_id} ({c.email}) desconectada")


# -- Navegação de pastas ------------------------------------------------------

@router.get("/accounts/{credential_id}/folders", response_model=list[FolderOut], dependencies=[Depends(require_api_key)])
async def list_root_folders(credential_id: int, db: Session = Depends(get_db)):
    c = _require_credential(credential_id, db)
    token = await _fresh_token(c, db)
    provider = _get_provider(c.provider)
    entries = await provider.list_root_folders(token)
    return [FolderOut(id=e.file_id, name=e.name, is_folder=e.is_folder) for e in entries if e.is_folder]


@router.get("/accounts/{credential_id}/folders/{folder_id}", response_model=list[FolderOut], dependencies=[Depends(require_api_key)])
async def list_subfolder(credential_id: int, folder_id: str, db: Session = Depends(get_db)):
    c = _require_credential(credential_id, db)
    token = await _fresh_token(c, db)
    provider = _get_provider(c.provider)
    entries = await provider.list_folder(token, folder_id)
    return [FolderOut(id=e.file_id, name=e.name, is_folder=e.is_folder) for e in entries]


# -- Jobs ---------------------------------------------------------------------

def _job_out(j: CloudBackupJob) -> JobOut:
    return JobOut(
        id=j.id,
        credential_id=j.credential_id,
        provider=j.credential.provider,
        email=j.credential.email,
        folder_id=j.folder_id,
        folder_name=j.folder_name,
        target_label=j.target_label,
        cron_expr=j.cron_expr,
        enabled=j.enabled,
        last_run_at=str(j.last_run_at) if j.last_run_at else None,
        last_run_status=j.last_run_status,
        last_run_message=j.last_run_message,
        created_at=str(j.created_at),
    )


@router.get("/jobs", response_model=list[JobOut], dependencies=[Depends(require_api_key)])
def list_jobs(db: Session = Depends(get_db)):
    return [_job_out(j) for j in db.query(CloudBackupJob).options(joinedload(CloudBackupJob.credential)).order_by(CloudBackupJob.created_at).all()]


@router.post("/jobs", response_model=JobOut, status_code=201, dependencies=[Depends(require_api_key)])
def create_job(req: JobCreate, db: Session = Depends(get_db)):
    _require_credential(req.credential_id, db)
    job = CloudBackupJob(
        credential_id=req.credential_id,
        folder_id=req.folder_id,
        folder_name=req.folder_name,
        target_label=req.target_label,
        cron_expr=req.cron_expr,
        enabled=req.enabled,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    if job.enabled and job.cron_expr:
        from scheduler import add_or_update_job
        add_or_update_job(job.id, job.cron_expr)
    log.info(f"[cloud] Job {job.id} criado: {job.folder_name} → {job.target_label} (cron={job.cron_expr})")
    return _job_out(job)


@router.get("/jobs/{job_id}", response_model=JobOut, dependencies=[Depends(require_api_key)])
def get_job(job_id: int, db: Session = Depends(get_db)):
    return _job_out(_require_job(job_id, db))


@router.patch("/jobs/{job_id}", response_model=JobOut, dependencies=[Depends(require_api_key)])
def update_job(job_id: int, req: JobUpdate, db: Session = Depends(get_db)):
    job = _require_job(job_id, db)
    if req.folder_id   is not None: job.folder_id   = req.folder_id
    if req.folder_name is not None: job.folder_name = req.folder_name
    if req.target_label is not None: job.target_label = req.target_label
    if req.cron_expr   is not None: job.cron_expr   = req.cron_expr
    if req.enabled     is not None: job.enabled     = req.enabled
    db.commit()

    from scheduler import add_or_update_job, remove_job
    if job.enabled and job.cron_expr:
        add_or_update_job(job.id, job.cron_expr)
    else:
        remove_job(job.id)

    log.info(f"[cloud] Job {job.id} atualizado")
    return _job_out(job)


@router.delete("/jobs/{job_id}", status_code=204, dependencies=[Depends(require_api_key)])
def delete_job(job_id: int, db: Session = Depends(get_db)):
    job = _require_job(job_id, db)
    from scheduler import remove_job
    remove_job(job.id)
    db.delete(job)
    db.commit()
    log.info(f"[cloud] Job {job_id} removido")


@router.post("/jobs/{job_id}/run", status_code=202, dependencies=[Depends(require_api_key)])
async def run_job_now(job_id: int, db: Session = Depends(get_db)):
    _require_job(job_id, db)
    lock = _job_locks.setdefault(job_id, asyncio.Lock())
    if lock.locked():
        raise HTTPException(409, "Job já está em execução")
    from cloud.runner import run_cloud_backup_job
    async def _run():
        async with lock:
            await run_cloud_backup_job(job_id)
    asyncio.create_task(_run())
    return {"status": "started", "job_id": job_id}


@router.get("/jobs/{job_id}/status", dependencies=[Depends(require_api_key)])
def job_status(job_id: int, db: Session = Depends(get_db)):
    job = _require_job(job_id, db)
    return {
        "job_id":          job.id,
        "last_run_at":     str(job.last_run_at) if job.last_run_at else None,
        "last_run_status": job.last_run_status,
        "last_run_message": job.last_run_message,
    }
