"""本地磁盘存储层：目录布局、原子提交、崩溃恢复扫描。

布局：
    <root>/objects/<enc(name)>.CURRENT          可见性指针（JSON，原子替换）
    <root>/objects/<enc(name)>.data.<token>     某一版本的对象数据
    <root>/objects/.staging/<token>             提交中的装配临时文件
    <root>/sessions/<sid>/session.json          会话元数据
    <root>/sessions/<sid>/chunks/<i>.part       已确认分片
    <root>/sessions/<sid>/chunks/<i>.tmp        写入中的分片（恢复时清除）

原子性约定：
    * 所有元数据文件都先写临时文件、fsync、再 os.replace 到目标路径。
    * 对象可见性由 CURRENT 指针决定：指针替换前读取方看到旧版本（或不存在），
      替换后看到新版本，绝不出现部分可见或混合版本。
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import IO, Iterator
from urllib.parse import quote, unquote

from .config import GatewayConfig
from .models import (
    ObjectPointer,
    SessionRecord,
    pointer_from_dict,
    pointer_to_dict,
    session_from_dict,
    session_to_dict,
)

_CHUNK_SUFFIX = ".part"
_CHUNK_TMP_SUFFIX = ".tmp"
_POINTER_SUFFIX = ".CURRENT"
_DATA_INFIX = ".data."


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Storage:
    def __init__(self, config: GatewayConfig):
        self.config = config
        self.root = Path(config.root_dir)

    # ---------- 目录布局 ----------

    @property
    def objects_dir(self) -> Path:
        return self.root / "objects"

    @property
    def staging_dir(self) -> Path:
        return self.objects_dir / ".staging"

    @property
    def sessions_dir(self) -> Path:
        return self.root / "sessions"

    def ensure_layout(self) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _encode_name(name: str) -> str:
        return quote(name, safe="")

    # ---------- 会话路径 ----------

    def session_dir(self, session_id: str) -> Path:
        return self.sessions_dir / self._encode_name(session_id)

    def session_meta_path(self, session_id: str) -> Path:
        return self.session_dir(session_id) / "session.json"

    def chunks_dir(self, session_id: str) -> Path:
        return self.session_dir(session_id) / "chunks"

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunks_dir(session_id) / f"{index}{_CHUNK_SUFFIX}"

    def chunk_tmp_path(self, session_id: str, index: int) -> Path:
        return self.chunks_dir(session_id) / f"{index}{_CHUNK_TMP_SUFFIX}"

    # ---------- 对象路径 ----------

    def pointer_path(self, object_name: str) -> Path:
        return self.objects_dir / (self._encode_name(object_name) + _POINTER_SUFFIX)

    def data_path(self, object_name: str, token: str) -> Path:
        return self.objects_dir / (
            self._encode_name(object_name) + _DATA_INFIX + token
        )

    def staging_path(self, token: str) -> Path:
        return self.staging_dir / token

    # ---------- 通用原子写 ----------

    def _atomic_write_bytes(self, final: Path, data: bytes) -> None:
        tmp = final.with_name(final.name + ".tmp")
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, final)
        _fsync_dir(final.parent)

    def atomic_write_stream(self, final: Path, reader: IO[bytes]) -> tuple[int, "object"]:
        """把 reader 流式写入 final（经临时文件 + rename）。

        返回 (字节数, 已更新的 hasher 占位)；调用方自行做摘要校验。
        内存占用以 copy_buffer_size 为界，与数据大小无关。
        """
        import hashlib

        buf_size = self.config.copy_buffer_size
        tmp = final.with_name(final.name + ".tmp")
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(tmp, "wb") as out:
                while True:
                    buf = reader.read(buf_size)
                    if not buf:
                        break
                    hasher.update(buf)
                    out.write(buf)
                    total += len(buf)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, final)
            _fsync_dir(final.parent)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return total, hasher

    # ---------- 会话元数据 ----------

    def save_session(self, record: SessionRecord) -> None:
        self.chunks_dir(record.session_id).mkdir(parents=True, exist_ok=True)
        payload = json.dumps(session_to_dict(record), sort_keys=True).encode()
        self._atomic_write_bytes(self.session_meta_path(record.session_id), payload)

    def load_session(self, session_id: str) -> SessionRecord | None:
        path = self.session_meta_path(session_id)
        if not path.exists():
            return None
        with open(path, "rb") as f:
            return session_from_dict(json.load(f))

    def list_sessions(self) -> Iterator[SessionRecord]:
        if not self.sessions_dir.exists():
            return
        for entry in sorted(self.sessions_dir.iterdir()):
            meta = entry / "session.json"
            if not meta.is_file():
                continue
            try:
                with open(meta, "rb") as f:
                    yield session_from_dict(json.load(f))
            except (OSError, ValueError, KeyError):
                # 元数据损坏的会话目录留给 GC 按目录清理，不让恢复整体失败
                continue

    def delete_session_dir(self, session_id: str) -> None:
        shutil.rmtree(self.session_dir(session_id), ignore_errors=True)

    # ---------- 对象指针与数据 ----------

    def read_pointer(self, object_name: str) -> ObjectPointer | None:
        path = self.pointer_path(object_name)
        if not path.exists():
            return None
        try:
            with open(path, "rb") as f:
                return pointer_from_dict(json.load(f))
        except (OSError, ValueError, KeyError):
            return None

    def commit_object(
        self, object_name: str, token: str, pointer: ObjectPointer
    ) -> ObjectPointer | None:
        """把 staging 中的装配结果原子发布为对象的新版本。

        步骤：staging -> data 文件（rename），fsync，再原子替换 CURRENT 指针。
        返回被替换掉的旧指针（调用方负责清理旧数据文件），无旧版本返回 None。
        """
        old = self.read_pointer(object_name)
        staged = self.staging_path(token)
        os.replace(staged, self.data_path(object_name, token))
        _fsync_dir(self.objects_dir)
        payload = json.dumps(pointer_to_dict(pointer), sort_keys=True).encode()
        self._atomic_write_bytes(self.pointer_path(object_name), payload)
        return old

    def remove_data_file(self, object_name: str, token: str) -> None:
        self.data_path(object_name, token).unlink(missing_ok=True)

    def open_object_data(self, pointer: ObjectPointer) -> IO[bytes]:
        return open(self.data_path(pointer.object_name, pointer.token), "rb")

    # ---------- 崩溃恢复 ----------

    def recover(self) -> None:
        """启动时清理崩溃残留，使磁盘回到一致状态。

        * 分片 *.tmp：写入未完成的临时分片，删除（对应分片视为未确认）。
        * objects/.staging/*：装配未完成的临时对象，删除（指针未切换，不可见）。
        * 无指针引用的 data 文件：提交中断或旧版本清理中断的残留，删除。
        """
        self.ensure_layout()
        # 1) 未完成的分片临时文件
        if self.sessions_dir.exists():
            for session_dir in self.sessions_dir.iterdir():
                chunks = session_dir / "chunks"
                if chunks.is_dir():
                    for f in chunks.iterdir():
                        if f.name.endswith(_CHUNK_TMP_SUFFIX):
                            f.unlink(missing_ok=True)
        # 2) 未完成的装配临时文件
        for f in self.staging_dir.iterdir():
            f.unlink(missing_ok=True)
        # 3) 收集所有指针引用的 data 文件，删除孤儿
        referenced: set[str] = set()
        for f in self.objects_dir.iterdir():
            if f.name.endswith(_POINTER_SUFFIX):
                name = unquote(f.name[: -len(_POINTER_SUFFIX)])
                ptr = self.read_pointer(name)
                if ptr is not None:
                    referenced.add(self.data_path(name, ptr.token).name)
        for f in self.objects_dir.iterdir():
            if _DATA_INFIX in f.name and f.name not in referenced:
                f.unlink(missing_ok=True)
