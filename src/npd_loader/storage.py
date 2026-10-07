"""File storage behind a stream interface so object stores can replace LocalStorage later."""
from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol
from urllib.parse import quote


class Storage(Protocol):
    def open_write(self, rel_path: str) -> BinaryIO: ...
    def open_read(self, rel_path: str) -> BinaryIO: ...
    def exists(self, rel_path: str) -> bool: ...
    def size(self, rel_path: str) -> int: ...
    def delete(self, rel_path: str) -> None: ...
    def rename(self, src_rel: str, dst_rel: str) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def uri(self, rel_path: str = "") -> str: ...


class LocalStorage:
    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root).resolve()

    def _path(self, rel_path: str) -> Path:
        posix = PurePosixPath(rel_path)
        if not rel_path or posix.is_absolute() or "\\" in rel_path or ".." in posix.parts:
            raise ValueError(f"unsafe storage path: {rel_path!r}")
        for part in posix.parts:
            if ":" in part:
                raise ValueError(f"unsafe storage path: {rel_path!r}")
        return self.root.joinpath(*posix.parts)

    def local_path(self, rel: str) -> str:
        return str(self._path(rel))

    def open_write(self, rel_path: str) -> BinaryIO:
        path = self._path(rel_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return open(path, "wb")

    def open_read(self, rel_path: str) -> BinaryIO:
        return open(self._path(rel_path), "rb")

    def exists(self, rel_path: str) -> bool:
        return self._path(rel_path).is_file()

    def size(self, rel_path: str) -> int:
        return self._path(rel_path).stat().st_size

    def delete(self, rel_path: str) -> None:
        self._path(rel_path).unlink(missing_ok=True)

    def rename(self, src_rel: str, dst_rel: str) -> None:
        dst = self._path(dst_rel)
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self._path(src_rel), dst)

    def list(self, prefix: str) -> list[str]:
        if not self.root.exists():
            return []
        found = []
        for path in self.root.rglob("*"):
            if path.is_file():
                rel = path.relative_to(self.root).as_posix()
                if rel.startswith(prefix):
                    found.append(rel)
        return sorted(found)

    def uri(self, rel_path: str = "") -> str:
        base = self.root.as_uri()
        if not rel_path:
            return base
        self._path(rel_path)
        return f"{base}/{quote(rel_path)}"
