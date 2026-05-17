"""Interface abstrata para provedores de cloud backup."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FileEntry:
    file_id: str
    name: str
    path: str        # caminho relativo dentro da pasta raiz (preenchido por list_folder_recursive)
    size: int
    mtime: float     # unix timestamp
    is_folder: bool = False


class CloudProvider(ABC):
    provider_name: str

    @abstractmethod
    def get_auth_url(self, redirect_uri: str, state: str, **kwargs) -> str:
        """Retorna URL de autorização OAuth2 para redirecionar o usuário."""

    @abstractmethod
    async def exchange_code(self, code: str, redirect_uri: str, **kwargs) -> dict:
        """Troca authorization code por tokens.
        Retorna: {"access_token", "refresh_token", "expiry" (datetime | None)}
        """

    @abstractmethod
    async def refresh_tokens(self, refresh_token: str) -> dict:
        """Renova access_token usando refresh_token.
        Retorna: {"access_token", "expiry" (datetime | None)}
        """

    @abstractmethod
    async def get_account_info(self, access_token: str) -> dict:
        """Retorna {"email", "display_name"}."""

    @abstractmethod
    async def list_root_folders(self, access_token: str) -> list[FileEntry]:
        """Lista pastas na raiz do Drive."""

    @abstractmethod
    async def list_folder(self, access_token: str, folder_id: str) -> list[FileEntry]:
        """Lista filhos imediatos (arquivos + subpastas) de um folder_id."""

    @abstractmethod
    async def download_file_to(
        self, access_token: str, file_id: str, dest_path: Path, chunk_size: int = 1024 * 1024
    ) -> tuple[str, int]:
        """Faz download do arquivo para dest_path calculando SHA-256 em single-pass.
        Retorna: (sha256_hex, size_bytes)
        """

    async def list_folder_recursive(
        self, access_token: str, folder_id: str, prefix: str = ""
    ) -> list[FileEntry]:
        """Lista todos os arquivos recursivamente, preenchendo FileEntry.path."""
        entries = await self.list_folder(access_token, folder_id)
        result: list[FileEntry] = []
        for e in entries:
            e.path = f"{prefix}/{e.name}".lstrip("/") if prefix else e.name
            if e.is_folder:
                children = await self.list_folder_recursive(access_token, e.file_id, e.path)
                result.extend(children)
            else:
                result.append(e)
        return result
