#!/usr/bin/env python3
# Copyright 2026 The Fast Sandbox Authors
# SPDX-License-Identifier: Apache-2.0
"""Create through the OpenSandbox SDK and submit Delete immediately at readiness.

Requires the OpenSandbox Python SDK. Set DOMAIN, API_KEY and TEMPLATE_ID.
N=300 CONCURRENCY=100 BATCH_PAUSE=5 REPORT_PATH=report.json python this_file.py
Delete timing ends at API acceptance; verify physical reclamation separately.
"""
import asyncio
import json
import math
import os
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from opensandbox.config import ConnectionConfig
from opensandbox.manager import SandboxManager
from opensandbox.sandbox import Sandbox


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {}
    return {
        "mean": statistics.mean(ordered),
        **{f"p{p}": ordered[math.ceil(p / 100 * len(ordered)) - 1] for p in (50, 90, 99)},
        "min": ordered[0], "max": ordered[-1],
    }


async def main():
    total = int(os.environ.get("N", "300"))
    concurrency = int(os.environ.get("CONCURRENCY", "10"))
    pause = float(os.environ.get("BATCH_PAUSE", "5"))
    assert total > 0 and concurrency > 0 and pause >= 0
    run_name = os.environ.get("RUN_NAME", f"c{concurrency}")
    config = ConnectionConfig(
        domain=os.environ["DOMAIN"], protocol=os.environ.get("PROTOCOL", "http"),
        api_key=os.environ["API_KEY"], request_timeout=timedelta(seconds=10),
        disable_metrics=True,
    )
    template = os.environ["TEMPLATE_ID"]
    manager = await SandboxManager.create(config)
    attempts = []
    started_utc = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()

    async def run(index):
        attempt = {"index": index, "create_success": False, "delete_accepted": False}
        sandbox = None
        before = time.monotonic()
        try:
            sandbox = await Sandbox.create_from_template(
                template, timeout=timedelta(hours=1), ready_timeout=timedelta(seconds=180),
                health_check_polling_interval=timedelta(milliseconds=10),
                connection_config=config,
                metadata={"origin": "delete-reliability-20261011", "run": run_name},
            )
            ready = time.monotonic()
            attempt.update(create_success=True, sandbox_id=sandbox.id, ready_seconds=ready-before)
            delete_start = time.monotonic()
            attempt["ready_to_delete_call_seconds"] = delete_start-ready
            await manager.kill_sandbox(sandbox.id)
            attempt.update(delete_accepted=True, delete_accept_seconds=time.monotonic()-delete_start)
        except Exception as error:
            attempt["error_type"] = type(error).__name__
            attempt["error_stage"] = "delete" if sandbox is not None else "create"
        finally:
            if sandbox is not None:
                try:
                    await sandbox.close()
                except Exception as error:
                    attempt["close_error_type"] = type(error).__name__
            attempt["completed_seconds"] = time.monotonic()-start
            attempts.append(attempt)
            print(json.dumps(attempt), flush=True)

    try:
        for first in range(1, total + 1, concurrency):
            await asyncio.gather(*(run(i) for i in range(first, min(first+concurrency, total+1))))
            if first+concurrency <= total:
                await asyncio.sleep(pause)
    finally:
        await manager.close()
    report = {
        "measurement": "SDK guest /ping readiness followed immediately by asynchronous Delete acceptance",
        "started_utc": started_utc, "finished_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {"n": total, "concurrency": concurrency, "batch_pause_seconds": pause,
                     "warmups": 0, "template_id": template, "request_timeout_seconds": 10,
                     "ready_timeout_seconds": 180, "ping_interval_ms": 10, "disable_metrics": True},
        "successes": sum(a["create_success"] for a in attempts),
        "failures": sum(not a["create_success"] for a in attempts),
        "delete_acceptances": sum(a["delete_accepted"] for a in attempts),
        "delete_failures": sum(a["create_success"] and not a["delete_accepted"] for a in attempts),
        "successful_latency_seconds": distribution([a["ready_seconds"] for a in attempts if a["create_success"]]),
        "delete_accept_seconds": distribution([a["delete_accept_seconds"] for a in attempts if a["delete_accepted"]]),
        "ready_to_delete_call_seconds": distribution([a["ready_to_delete_call_seconds"] for a in attempts if a["create_success"]]),
        "wall_seconds_including_batch_pauses": time.monotonic()-start,
        "percentile_method": "nearest_rank", "attempts": sorted(attempts, key=lambda a: a["index"]),
    }
    Path(os.environ["REPORT_PATH"]).write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps({k: v for k, v in report.items() if k != "attempts"}), flush=True)
    if report["failures"] or report["delete_failures"]:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
