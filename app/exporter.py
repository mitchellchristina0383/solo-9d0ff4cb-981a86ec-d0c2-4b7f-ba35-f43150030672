"""完整价目表导出：两条连接、同一快照、分批流式落盘。

- 连接 A 开启 REPEATABLE READ 只读事务并 `pg_export_snapshot()`；
- 连接 B 开启同级事务并 `SET TRANSACTION SNAPSHOT` 导入同一快照——
  两段读到的是同一个一致性快照：一笔同时改动两段价格的事务，在 CSV
  里只会整体呈现改前、或整体呈现改后的价格；
- 每段各用一个服务端游标分批取数，当前批次立即写入各自的临时 spool
  文件，内存中只保留当前批次；
- 两段都读完后才拼接交付：段 1 的 id 全部 <= 分界点 < 段 2，段内按
  id 升序，顺序拼接即全局有序；
- 任一段失败：TaskGroup 取消另一段，两路连接都按“脏”交还连接池
  （由连接池做服务端取消 / 回滚 / 会话验证后再复用或更换）。
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import shutil
from dataclasses import dataclass, field

from pool import AcquireTimeoutError, AsyncPool, PoolClosedError

logger = logging.getLogger("export")

# pg_export_snapshot() 的返回形如 '00000004-0000001F-1'
_SNAPSHOT_RE = re.compile(r"^[0-9A-Fa-f-]+$")
_COPY_CHUNK = 64 * 1024

# 两段的分界条件（固定字面量；分界点是 int，拼接无注入面）
_WHERE = {1: "id <=", 2: "id >"}


class ExportFailed(Exception):
    """任一段读取失败（或快照建立失败），导出整体失败。"""


@dataclass
class ExportResult:
    path: str
    rows: dict[int, int] = field(default_factory=dict)     # 段号 -> 行数
    batches: dict[int, int] = field(default_factory=dict)  # 段号 -> 批次数


def _flatten(exc: BaseException) -> str:
    if isinstance(exc, BaseExceptionGroup):
        return "; ".join(_flatten(e) for e in exc.exceptions)
    return f"{type(exc).__name__}: {exc}"


async def _read_segment(
    conn,
    seg: int,
    boundary: int,
    path: str,
    batch_size: int,
    fail_after_batch: int | None,
) -> tuple[int, int]:
    """在已开启的快照事务里，用服务端游标分批读出一段并写入 spool 文件。

    内存中只保留当前批次；返回 (行数, 批次数)。
    """
    rows_total = 0
    batches = 0
    async with conn.cursor(name=f"pricelist_seg{seg}") as cur:
        await cur.execute(
            "SELECT id, name, price FROM items "
            f"WHERE {_WHERE[seg]} {boundary} ORDER BY id")
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            while True:
                rows = await cur.fetchmany(batch_size)
                if not rows:
                    break
                writer.writerows(rows)
                rows_total += len(rows)
                batches += 1
                if fail_after_batch is not None and batches >= fail_after_batch:
                    raise ExportFailed(
                        f"segment {seg} failed "
                        f"(injected after {batches} batch(es))")
    return rows_total, batches


async def export_pricelist(
    pool: AsyncPool,
    *,
    boundary: int,
    batch_size: int,
    workdir: str,
    demo_fail_segment: int | None = None,
    acquire_timeout: float | None = 10.0,
) -> ExportResult:
    """用两条池化连接并行导出完整价目表，返回已落盘的 CSV 信息。

    无论成功失败，两条连接都会归还连接池；失败时按“脏”归还，由
    连接池清理/更换。临时文件（workdir）由调用方负责清理。
    """
    seg_paths = {s: os.path.join(workdir, f"seg{s}.csv") for s in (1, 2)}
    final_path = os.path.join(workdir, "pricelist.csv")
    conn_a = conn_b = None
    ok = False
    try:
        conn_a = await pool.acquire(acquire_timeout)
        try:
            conn_b = await pool.acquire(acquire_timeout)
        except BaseException:
            await pool.release(conn_a, dirty=False)  # 尚未使用，干净归还
            conn_a = None
            raise

        # 连接 A：REPEATABLE READ 只读事务，导出一致性快照
        await conn_a.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur = await conn_a.execute("SELECT pg_export_snapshot()")
        snapshot_id = (await cur.fetchone())[0]
        if not _SNAPSHOT_RE.fullmatch(snapshot_id):
            raise ExportFailed(f"unexpected snapshot id: {snapshot_id!r}")

        fail_after = {1: None, 2: None}
        if demo_fail_segment in (1, 2):
            fail_after[demo_fail_segment] = 1  # 演示/自测：读完第一批后失败

        async def seg1():
            return await _read_segment(
                conn_a, 1, boundary, seg_paths[1], batch_size, fail_after[1])

        async def seg2():
            # 连接 B：同级只读事务，导入连接 A 的快照 -> 两段同一快照
            await conn_b.execute(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            await conn_b.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}'")
            return await _read_segment(
                conn_b, 2, boundary, seg_paths[2], batch_size, fail_after[2])

        try:
            async with asyncio.TaskGroup() as tg:
                t1 = tg.create_task(seg1())
                t2 = tg.create_task(seg2())
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # 任一段失败 -> 整体失败
            raise ExportFailed(f"segment read failed: {_flatten(exc)}") from exc

        # 两段都读完：结束两路事务（只读事务，COMMIT 与 ROLLBACK 等价）
        for conn in (conn_a, conn_b):
            await conn.execute("COMMIT")
        ok = True

        result = ExportResult(path=final_path)
        for seg, task in ((1, t1), (2, t2)):
            result.rows[seg], result.batches[seg] = task.result()

        # 拼接交付：表头 + 段 1 + 段 2（段 1 的 id 全部 <= 分界点 < 段 2，
        # 段内升序，顺序拼接即全局按 id 有序）；流式拷贝，不占内存
        with open(final_path, "w", encoding="utf-8", newline="") as out:
            out.write("id,name,price\n")
            for seg in (1, 2):
                with open(seg_paths[seg], encoding="utf-8") as part:
                    shutil.copyfileobj(part, out, _COPY_CHUNK)
        logger.info("export done: rows=%s batches=%s",
                    result.rows, result.batches)
        return result
    except (AcquireTimeoutError, PoolClosedError):
        raise
    except asyncio.CancelledError:
        raise
    except ExportFailed:
        raise
    except Exception as exc:
        raise ExportFailed(f"{type(exc).__name__}: {exc}") from exc
    finally:
        for conn in (conn_a, conn_b):
            if conn is not None:
                await pool.release(conn, dirty=not ok)
