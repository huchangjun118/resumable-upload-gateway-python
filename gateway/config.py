"""网关配置项。"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GatewayConfig:
    root_dir: Path | str = "./data"
    # 每个 owner 的默认配额（字节），含已提交对象与进行中会话的已确认分片
    default_quota_bytes: int = 1 << 30
    # 会话存活时间（秒），超时未完成的会话可被回收为 EXPIRED
    session_ttl_seconds: float = 24 * 3600
    # 流式拷贝缓冲区大小；单对象处理内存开销以此为界，不随对象大小增长
    copy_buffer_size: int = 1 << 20
    # 单个分片允许的最大尺寸
    max_chunk_size: int = 64 << 20
    # 已提交对象旧版本数据文件的保留：提交成功后立即清理
    per_owner_quota: dict[str, int] = field(default_factory=dict)

    def quota_for(self, owner: str) -> int:
        return self.per_owner_quota.get(owner, self.default_quota_bytes)
