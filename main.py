#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified entrypoint for departure duty scheduling.

Run:
  python main.py --input xxx.xlsx --output out.xlsx

Optional:
  --report out_report.txt
  --dry-run
  --make-sample sample_input.xlsx
"""
from __future__ import annotations

import argparse
from pathlib import Path

from departure_duty_scheduler import SolverSettings, generate_sample_input, run_pipeline


# =========================
# 設定區（集中調參）
# =========================
SETTINGS = SolverSettings(
    # Soft weights
    weight_last_hour_work=50,      # 下班前 1 小時排班懲罰
    weight_group_fairness=8,       # 同班段公平懲罰
    weight_target_deviation=3,     # 個人工時目標偏差懲罰
    weight_same_hour_consistency=12,  # 同一小時盡量同人同崗
    weight_single_slot_fragment=18,   # 單一30分鐘碎片懲罰
    weight_shortage_slot=100000,   # shortage 模式缺口懲罰（每人*每格）

    # Hard-rule limits
    auto_gate_max_slots=6,         # 自動通關上限: 6 格 = 3 小時
    max_consecutive_work_slots=6,  # 連續上班上限: 6 格 = 3 小時
    early_max_work_slots=14,       # 早班(05/06)工時參考上限: 14 格 = 7 小時
    late_max_work_slots=15,        # 晚班(07/08)工時參考上限: 15 格 = 7.5 小時
    enforce_shift_work_caps=False, # False=軟限制(可超時但吃懲罰), True=硬限制
    weight_shift_cap_excess=30,    # 超過早晚班參考上限的懲罰係數

    # Feasibility strategy
    # - "hard": 需求必滿足；若不可行會自動再跑 allow_shortage 產生缺口摘要
    # - "allow_shortage": 允許缺口並懲罰 shortage
    feasibility_mode="hard",

    # Solver budget
    max_time_sec=30,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Departure Duty Scheduler (CP-SAT)")
    parser.add_argument("--input", default="", help="input excel path")
    parser.add_argument("--output", default="", help="output excel path")
    parser.add_argument("--report", default="", help="output report txt path")
    parser.add_argument("--dry-run", action="store_true", help="validate + diagnostics only, no solving")
    parser.add_argument("--make-sample", default="", help="generate sample input excel and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.make_sample:
        sample_path = generate_sample_input(args.make_sample)
        print(f"sample_input_generated={sample_path}")
        return

    if not args.input or not args.output:
        raise SystemExit("請提供 --input 與 --output")

    output_excel = args.output
    report_path = args.report
    if not report_path:
        p = Path(output_excel)
        report_path = str((p.parent / f"{p.stem}_report.txt").resolve())

    result = run_pipeline(
        input_path=args.input,
        output_excel_path=output_excel,
        report_path=report_path,
        settings=SETTINGS,
        dry_run=bool(args.dry_run),
    )

    print("=== RUN RESULT ===")
    for k in [
        "status",
        "mode_used",
        "all_covered",
        "total_shortage_slots",
        "dry_total_demand_slots",
        "dry_total_on_duty_slots",
        "dry_skill_gap_rows",
        "output_excel",
        "report",
    ]:
        if k in result:
            print(f"{k}={result[k]}")


if __name__ == "__main__":
    main()
