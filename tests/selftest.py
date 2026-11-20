#!/usr/bin/env python3
"""只读查询 API 的端到端自测。

前置条件：
- API 已在 BASE_URL（默认 http://127.0.0.1:8000）运行，POOL_SIZE=4；
- 价目表导出用例需要直连数据库：环境变量 PGDSN 给出连接串
  （默认 postgresql://app:app@127.0.0.1:5432/demo，即 compose 映射的地址）；
- 可选：环境变量 PSQL 给出 psql 连接串（如
  "postgresql://app:app@127.0.0.1:5432/demo"），用于服务端核对
  （连接数上限、无残留活跃查询、无失败事务）；未提供则跳过这些检查。

用法：python3 tests/selftest.py
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import random
import subprocess
import sys
import threading
import time
from contextlib import suppress

import httpx
import psycopg

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
POOL_SIZE = int(os.environ.get("POOL_SIZE", "4"))
PSQL = os.environ.get("PSQL")  # 例如 postgresql://app:app@127.0.0.1:5432/demo
PGDSN = os.environ.get(
    "PGDSN", "postgresql://app:app@127.0.0.1:5432/demo")  # 导出用例直连库

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(f"{name}: {detail}")


async def stats(client: httpx.AsyncClient) -> dict:
    return (await client.get("/pool/stats")).json()


def server_count(where: str = "") -> int | None:
    """通过 pg_stat_activity 做服务端核对；无 psql 时返回 None。"""
    if not PSQL:
        return None
    sql = ("SELECT count(*) FROM pg_stat_activity "
           "WHERE application_name = 'ro-api' " + where)
    try:
        out = subprocess.run(
            ["psql", PSQL, "-tAc", sql],
            capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            return None
        return int(out.stdout.strip())
    except Exception:
        return None


class Sampler:
    """高频采样 /pool/stats，校验不变量：
    idle + in_use + cleaning == size 且 opened_connections <= size。"""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.violations: list[str] = []
        self.max_in_use = 0
        self.max_waiting = 0
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                s = (await self.client.get("/pool/stats")).json()
                self.max_in_use = max(self.max_in_use, s["in_use"])
                self.max_waiting = max(self.max_waiting, s["waiting"])
                total = s["idle"] + s["in_use"] + s["cleaning"]
                if total != s["size"]:
                    self.violations.append(f"slot leak: {s}")
                if s["in_use"] > s["size"] or s["opened_connections"] > s["size"]:
                    self.violations.append(f"limit exceeded: {s}")
            except Exception as exc:  # noqa: BLE001
                self.violations.append(f"sampler error: {exc}")
            await asyncio.sleep(0.05)

    def stop(self) -> None:
        self._stop.set()


async def wait_pool_idle(client: httpx.AsyncClient, timeout: float = 15) -> dict:
    deadline = time.monotonic() + timeout
    s = await stats(client)
    while time.monotonic() < deadline:
        if s["idle"] == POOL_SIZE and s["cleaning"] == 0 and s["waiting"] == 0:
            break
        await asyncio.sleep(0.1)
        s = await stats(client)
    return s


# ---------------------------------------------------------------- 用例

def db_query(sql: str, params: tuple = ()) -> list:
    """直连数据库查询（供导出用例核对/造数）。"""
    with psycopg.connect(PGDSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def db_exec(sql: str, params: tuple = ()) -> None:
    """直连数据库执行并提交。"""
    with psycopg.connect(PGDSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def csv_rows(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


def csv_prices(text: str) -> dict[str, str]:
    return {r[0]: r[2] for r in csv_rows(text)[1:]}


async def t_normal(client):
    r = await client.post("/query/normal", json={"limit": 3})
    check("normal: 200", r.status_code == 200, r.text)
    if r.status_code == 200:
        check("normal: 3 rows", len(r.json().get("rows", [])) == 3, r.text)


async def t_queue(client):
    """8 个慢查询、池大小 4：4 个执行、4 个排队，最终全部完成。"""
    sampler = Sampler(client)
    stask = asyncio.create_task(sampler.run())
    reqs = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 2, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(8)]
    await asyncio.sleep(0.7)
    mid = await stats(client)
    check("queue: in_use == size", mid["in_use"] == POOL_SIZE, str(mid))
    check("queue: waiting == 4", mid["waiting"] == 4, str(mid))
    results = await asyncio.gather(*reqs)
    sampler.stop()
    await stask
    codes = [r.status_code for r in results]
    check("queue: all 8 finished 200", all(c == 200 for c in codes), str(codes))
    check("queue: invariants held", not sampler.violations,
          "; ".join(sampler.violations[:3]))
    s = await wait_pool_idle(client)
    check("queue: pool idle again", s["idle"] == POOL_SIZE, str(s))


async def t_acquire_timeout(client):
    """池被占满时，acquire_timeout=1 的请求应在约 1 秒后拿到 503。"""
    blockers = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 3, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(POOL_SIZE)]
    await asyncio.sleep(0.7)
    t0 = time.monotonic()
    r = await client.post("/query/slow", json={
        "delay_seconds": 0.1, "acquire_timeout": 1, "query_timeout": 5})
    dt = time.monotonic() - t0
    check("acquire-timeout: 503", r.status_code == 503,
          f"{r.status_code} {r.text}")
    check("acquire-timeout: ~1s", 0.8 < dt < 2.5, f"{dt:.2f}s")
    results = await asyncio.gather(*blockers)
    check("acquire-timeout: blockers finished",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))
    await wait_pool_idle(client)


async def t_queue_cancel(client):
    """排队中的请求被客户端取消后，队列名额应立即释放。"""
    blockers = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 2.5, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(POOL_SIZE)]
    await asyncio.sleep(0.7)
    before = await stats(client)
    queued = asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 0.1, "acquire_timeout": 30, "query_timeout": 5}))
    await asyncio.sleep(0.5)
    mid = await stats(client)
    check("cancel: waiting grew by 1",
          mid["waiting"] == before["waiting"] + 1, f"{before} -> {mid}")
    queued.cancel()  # 客户端放弃 -> 服务端应观测到断开
    with suppress(asyncio.CancelledError):
        await queued
    deadline = time.monotonic() + 5
    s = await stats(client)
    while time.monotonic() < deadline and s["waiting"] != before["waiting"]:
        await asyncio.sleep(0.1)
        s = await stats(client)
    check("cancel: queue slot released", s["waiting"] == before["waiting"],
          str(s))
    check("cancel: counter bumped",
          s["counters"]["queue_cancelled"] > before["counters"]["queue_cancelled"],
          str(s["counters"]))
    results = await asyncio.gather(*blockers)
    check("cancel: blockers still finish",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))
    await wait_pool_idle(client)


async def t_query_timeout(client):
    """query_timeout=1 的慢查询：504；连接不得带伤复用，池应恢复。"""
    before = await stats(client)
    t0 = time.monotonic()
    r = await client.post("/query/slow", json={
        "delay_seconds": 10, "acquire_timeout": 5, "query_timeout": 1})
    dt = time.monotonic() - t0
    check("query-timeout: 504", r.status_code == 504,
          f"{r.status_code} {r.text}")
    check("query-timeout: ~1s", 0.8 < dt < 3, f"{dt:.2f}s")
    s = await wait_pool_idle(client)
    check("query-timeout: pool recovered", s["idle"] == POOL_SIZE, str(s))
    c, b = s["counters"], before["counters"]
    check("query-timeout: recycled or replaced",
          c["recycled"] + c["replaced"] > b["recycled"] + b["replaced"], str(c))
    n = server_count()
    if n is not None:
        check("query-timeout: server conns <= size", n <= POOL_SIZE, f"n={n}")
    active = server_count("AND state = 'active'")
    if active is not None:
        check("query-timeout: nothing running server-side", active == 0,
              f"active={active}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("query-timeout: normal query after", r.status_code == 200, r.text)


async def t_sql_error(client):
    """两种 SQL 报错：连接须清理/更换后才能复用，后续查询不受影响。"""
    before = await stats(client)
    r = await client.post("/query/error", json={"mode": "autocommit"})
    check("sql-error: autocommit -> 400", r.status_code == 400,
          f"{r.status_code} {r.text}")
    r = await client.post("/query/error", json={"mode": "transaction"})
    check("sql-error: transaction -> 400", r.status_code == 400,
          f"{r.status_code} {r.text}")
    s = await wait_pool_idle(client)
    c, b = s["counters"], before["counters"]
    check("sql-error: 2 connections sanitized",
          c["recycled"] + c["replaced"] >= b["recycled"] + b["replaced"] + 2,
          f"{b} -> {c}")
    aborted = server_count("AND state = 'idle in transaction (aborted)'")
    if aborted is not None:
        check("sql-error: no failed transaction left", aborted == 0,
              f"aborted={aborted}")
    # 池里每条连接都可能被轮到，多发几个普通查询验证没有残留事务
    results = await asyncio.gather(*[
        client.post("/query/normal", json={"limit": 2}) for _ in range(8)])
    check("sql-error: normal queries after",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))


async def t_disconnect(client):
    """客户端在慢查询执行中断开：连接不得带伤复用，池应恢复。"""
    before = await stats(client)
    req = asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 10, "acquire_timeout": 5, "query_timeout": 30}))
    await asyncio.sleep(0.7)
    req.cancel()
    with suppress(asyncio.CancelledError):
        await req
    s = await wait_pool_idle(client)
    check("disconnect: pool recovered", s["idle"] == POOL_SIZE, str(s))
    check("disconnect: counter bumped",
          s["counters"]["client_disconnects"]
          > before["counters"]["client_disconnects"], str(s["counters"]))
    active = server_count("AND state = 'active'")
    if active is not None:
        check("disconnect: nothing running server-side", active == 0,
              f"active={active}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("disconnect: normal query after", r.status_code == 200, r.text)


async def t_stress(client):
    """取消与归还交错的压力测试：连接数不得越限，其他请求照常完成。"""
    sampler = Sampler(client)
    stask = asyncio.create_task(sampler.run())
    rng = random.Random(42)

    async def one(i: int):
        kind = i % 5
        try:
            if kind == 0:
                return await client.post("/query/error", json={
                    "mode": "transaction" if i % 10 == 0 else "autocommit"})
            if kind in (1, 2):
                return await client.post("/query/slow", json={
                    "delay_seconds": rng.uniform(0.3, 1.5),
                    "acquire_timeout": rng.choice([0.5, 2, 30]),
                    "query_timeout": rng.choice([0.5, 2, 30])})
            return await client.post("/query/normal", json={
                "limit": 3,
                "acquire_timeout": rng.choice([0.5, 30]),
                "query_timeout": 10})
        except Exception:  # noqa: BLE001 - 客户端侧异常不计失败
            return None

    async def aborted(_: int):
        t = asyncio.create_task(client.post("/query/slow", json={
            "delay_seconds": 3, "acquire_timeout": 30, "query_timeout": 30}))
        await asyncio.sleep(rng.uniform(0.1, 0.8))
        t.cancel()
        with suppress(asyncio.CancelledError):
            await t

    tasks = ([one(i) for i in range(30)]
             + [aborted(i) for i in range(6)])
    results = await asyncio.gather(*tasks)
    sampler.stop()
    await stask
    ok = sum(1 for r in results
             if r is not None and r.status_code == 200)
    check("stress: some requests completed", ok >= 10, f"ok={ok}")
    check("stress: invariants held", not sampler.violations,
          "; ".join(sampler.violations[:5]))
    s = await wait_pool_idle(client, timeout=20)
    check("stress: pool fully recovered",
          s["idle"] == POOL_SIZE and s["cleaning"] == 0 and s["waiting"] == 0,
          str(s))
    check("stress: opened == size", s["opened_connections"] == POOL_SIZE,
          str(s))
    n = server_count()
    if n is not None:
        check("stress: server conns == size", n == POOL_SIZE, f"n={n}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("stress: normal query after", r.status_code == 200, r.text)
    print("  stress counters:", s["counters"])


# ---------------------------------------------------------------- 价目表导出

async def t_export_basic(client):
    """基本导出：CSV 完整、按编号排序、内容与库一致；边界外仍完整。"""
    rows_db = await asyncio.to_thread(
        db_query, "SELECT id, name, price FROM items ORDER BY id")
    r = await client.get("/export/pricelist",
                         params={"boundary": 10, "batch_size": 1})
    ok = (r.status_code == 200
          and r.headers.get("content-type", "").startswith("text/csv"))
    check("export: 200 text/csv", ok, f"{r.status_code} {r.text[:200]}")
    if not ok:
        return
    rows = csv_rows(r.text)
    check("export: header id,name,price",
          rows[0] == ["id", "name", "price"], str(rows[0]))
    got = rows[1:]
    ids = [int(x[0]) for x in got]
    check("export: row count matches db",
          len(got) == len(rows_db), f"{len(got)} vs {len(rows_db)}")
    check("export: sorted by id", ids == sorted(ids))
    expect = [[str(i), n, str(p)] for i, n, p in rows_db]
    check("export: content matches db", got == expect)
    check("export: one batch per row (batch_size=1)",
          r.headers.get("x-export-batches") == str(len(rows_db)),
          r.headers.get("x-export-batches", "?"))
    for b in (0, 100000):  # 分界点在编号范围之外：一段为空，导出仍完整
        r = await client.get("/export/pricelist", params={"boundary": b})
        n = len(csv_rows(r.text)) - 1 if r.status_code == 200 else -1
        check(f"export: boundary={b} still complete",
              r.status_code == 200 and n == len(rows_db),
              f"{r.status_code} rows={n}")


async def t_export_snapshot(client):
    """并发改价：改价事务在导出中途提交，CSV 只能全改前或全改后。"""
    boundary = 10
    extra = 20000  # 额外商品，让第 2 段分批读取持续数秒
    await asyncio.to_thread(
        db_exec,
        "INSERT INTO items (name, price) "
        "SELECT 'bulk-' || g, round((random() * 100)::numeric, 2) "
        "FROM generate_series(1, %s) AS g", (extra,))
    base = dict(await asyncio.to_thread(db_query, "SELECT id, price FROM items"))
    writer_err: list[BaseException] = []

    def reprice():  # 一笔事务同时改两段（此处为全表）的价格并提交
        try:
            db_exec("UPDATE items SET price = price + 100")
        except BaseException as exc:  # noqa: BLE001
            writer_err.append(exc)

    try:
        # batch_size=1：第 2 段 2 万余行逐批读取，导出持续数秒
        export = asyncio.create_task(client.get(
            "/export/pricelist",
            params={"boundary": boundary, "batch_size": 1}))
        await asyncio.sleep(0.7)   # 快照已建立，第 2 段仍在分批读取
        await asyncio.to_thread(reprice)  # 改价事务在导出中途提交
        r1 = await export
        check("snapshot: export 200", r1.status_code == 200, r1.text[:200])
        check("snapshot: writer ok", not writer_err, str(writer_err))
        if r1.status_code == 200:
            got = csv_prices(r1.text)
            expect_old = {str(i): str(p) for i, p in base.items()}
            n_diff = sum(1 for k, v in expect_old.items() if got.get(k) != v)
            check("snapshot: rows complete", len(got) == len(base),
                  f"{len(got)} vs {len(base)}")
            check("snapshot: commit mid-export -> ALL old prices",
                  got == expect_old, f"{n_diff}/{len(base)} rows differ")
            check("snapshot: many batches",
                  int(r1.headers.get("x-export-batches", "0")) >= extra,
                  r1.headers.get("x-export-batches", "?"))
        r2 = await client.get("/export/pricelist",
                              params={"boundary": boundary})
        check("snapshot: second export 200", r2.status_code == 200,
              r2.text[:200])
        if r2.status_code == 200:
            got2 = csv_prices(r2.text)
            expect_new = {str(i): str(p + 100) for i, p in base.items()}
            check("snapshot: after commit -> ALL new prices",
                  got2 == expect_new)
    finally:
        # 恢复：还原价格、删除额外商品，保证自测可重复
        def restore():
            with psycopg.connect(PGDSN) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        "UPDATE items SET price = %s WHERE id = %s",
                        [(p, i) for i, p in base.items()])
                    cur.execute("DELETE FROM items WHERE name LIKE 'bulk-%'")
                    cur.execute(
                        "SELECT setval('items_id_seq', "
                        "(SELECT max(id) FROM items))")
                conn.commit()
        await asyncio.to_thread(restore)


async def t_export_failure(client):
    """单路失败：任一段失败 -> 500，两路连接都释放，服务随后照常。"""
    before = await stats(client)
    for seg in (1, 2):
        r = await client.get("/export/pricelist", params={
            "boundary": 10, "demo_fail_segment": seg})
        body = r.json() if r.headers.get("content-type", "").startswith(
            "application/json") else {}
        check(f"fail-seg{seg}: 500 export_failed",
              r.status_code == 500 and body.get("error") == "export_failed",
              f"{r.status_code} {r.text[:200]}")
        check(f"fail-seg{seg}: detail names segment",
              f"segment {seg}" in body.get("detail", ""), body.get("detail", ""))
    s = await wait_pool_idle(client)
    check("fail: pool fully recovered",
          s["idle"] == POOL_SIZE and s["cleaning"] == 0 and s["waiting"] == 0,
          str(s))
    check("fail: opened connections == size",
          s["opened_connections"] == POOL_SIZE, str(s))
    c, b = s["counters"], before["counters"]
    check("fail: dirty connections sanitized",
          c["recycled"] + c["replaced"] >= b["recycled"] + b["replaced"] + 2,
          f"{b} -> {c}")
    # 服务端核对：两路连接均已释放，无残留事务 / 活跃查询
    acts = await asyncio.to_thread(
        db_query,
        "SELECT state, count(*) FROM pg_stat_activity "
        "WHERE application_name = 'ro-api' GROUP BY state")
    states = {st or "null": n for st, n in acts}
    check("fail: no leftover transaction server-side",
          not any(st.startswith("idle in transaction") for st in states),
          str(states))
    check("fail: nothing active server-side",
          states.get("active", 0) == 0, str(states))
    check("fail: server conns == pool size",
          sum(states.values()) == POOL_SIZE, str(states))
    r = await client.get("/export/pricelist", params={"boundary": 10})
    check("fail: export works after", r.status_code == 200, f"{r.status_code}")


# ---------------------------------------------------------------- 入口

async def main() -> int:
    timeout = httpx.Timeout(60.0)
    async with httpx.AsyncClient(base_url=BASE, timeout=timeout) as client:
        for _ in range(60):
            try:
                if (await client.get("/pool/stats")).status_code == 200:
                    break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)
        else:
            print(f"API not reachable at {BASE}")
            return 2
        print(f"API up at {BASE}, pool size = {POOL_SIZE}, "
              f"psql checks: {'on' if PSQL else 'off'}")
        await t_normal(client)
        await t_queue(client)
        await t_acquire_timeout(client)
        await t_queue_cancel(client)
        await t_query_timeout(client)
        await t_sql_error(client)
        await t_disconnect(client)
        await t_stress(client)
        await t_export_basic(client)
        await t_export_snapshot(client)
        await t_export_failure(client)
    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(" -", f)
        return 1
    print("ALL TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
