#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _tail_text(text: str | None, limit: int = 2000) -> str:
    if not text:
        return ""
    return text[-limit:]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run A/B research in batch")
    parser.add_argument("--groups-file", type=str, required=True, help="CSV with column group")
    parser.add_argument("--market-duckdb", type=str, required=True)
    parser.add_argument("--index-duckdb", type=str, required=True)
    parser.add_argument("--output-dir", type=str, default="data/tasks/research_runner")
    parser.add_argument("--train-end", type=str, default="2025-12-31")
    parser.add_argument("--test-start", type=str, default="2026-01-01")
    parser.add_argument("--test-end", type=str, default="2026-03-10")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--model-type", type=str, default="ridge", choices=["lstsq", "ridge", "huber"])
    parser.add_argument("--timeout-seconds", type=int, default=7200, help="Per-group subprocess timeout")
    args = parser.parse_args()

    groups = pd.read_csv(args.groups_file)["group"].astype(str).tolist()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for group in groups:
        cmd = [
            sys.executable, "-m", "timesfm_cn_forecast.run_group_eval",
            "--group", group,
            "--market-duckdb", args.market_duckdb,
            "--index-duckdb", args.index_duckdb,
            "--train-end", args.train_end,
            "--test-start", args.test_start,
            "--test-end", args.test_end,
            "--sample-size", str(args.sample_size),
            "--model-type", args.model_type,
            "--output-dir", str(out_dir),
        ]
        status = "ok"
        message = ""
        try:
            ret = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=args.timeout_seconds,
            )
            if ret.returncode != 0:
                status = "error"
                message = "\n".join(
                    part
                    for part in [
                        f"returncode={ret.returncode}",
                        _tail_text(ret.stderr),
                        _tail_text(ret.stdout),
                    ]
                    if part
                )
        except subprocess.TimeoutExpired as exc:
            status = "timeout"
            message = "\n".join(
                part
                for part in [
                    f"timeout_seconds={args.timeout_seconds}",
                    _tail_text(exc.stderr if isinstance(exc.stderr, str) else None),
                    _tail_text(exc.stdout if isinstance(exc.stdout, str) else None),
                ]
                if part
            )
        except Exception as exc:  # pragma: no cover - defensive guard for batch stability
            status = "error"
            message = f"{type(exc).__name__}: {exc}"

        rows.append({"group": group, "status": status, "message": message})

    pd.DataFrame(rows).to_csv(out_dir / "ab_summary.csv", index=False)
    print(f"Saved: {out_dir / 'ab_summary.csv'}")


if __name__ == "__main__":
    main()
