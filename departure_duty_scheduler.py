#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Departure duty scheduling core (OR-Tools CP-SAT).

Excel format:
- Sheet `Employees`
  Required columns:
    - name
    - shift_start: one of 05:00 / 06:00 / 07:00 / 08:00
    - shift_end: one of 16:00 / 17:00 / 19:00 / 20:00
  Skill columns (choose one style):
    - skills: comma-separated role names
    - or role/bool columns prefixed by `skill_`, e.g. skill_公務台, skill_查驗台1
      - value `0/空白`: 不可排該崗位
      - value `1`: 可排該崗位
      - value `2`: 可排且為該崗位專責（偏好指派）
  Optional:
    - target_work_minutes

- Sheet `Demand`
  Required columns:
    - time: 30-min slots from 05:00 ... 19:30
    - role columns: each column is a role name, value is required headcount (>=0)
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

from ortools.sat.python import cp_model


VALID_WINDOWS = {
    ("05:00", "16:00"),
    ("06:00", "17:00"),
    ("07:00", "19:00"),
    ("08:00", "20:00"),
}

SHIFT_START_TO_END = {
    "05:00": "16:00",
    "06:00": "17:00",
    "07:00": "19:00",
    "08:00": "20:00",
}

EMPLOYEE_NAME_ALIASES = ["name", "姓名"]
EMPLOYEE_SHIFT_START_ALIASES = ["shift_start", "上勤時間", "上勤開始時間"]
EMPLOYEE_SHIFT_END_ALIASES = ["shift_end", "下勤時間", "上勤結束時間", "下勤結束時間"]
EMPLOYEE_SKILLS_ALIASES = ["skills", "技能", "職能"]
EMPLOYEE_TARGET_MIN_ALIASES = ["target_work_minutes", "目標工時分鐘", "目標分鐘數"]
DEMAND_TIME_ALIASES = ["time", "時間"]

ROLE_ALIASES = {
    "自通": "自動通關",
    "自動通關": "自動通關",
    "公務台協勤": "公協",
    "公協": "公協",
}

ROLE_DISPLAY_NAMES = {
    "公務台": "公務檯",
    "公協": "公務檯協勤引導",
    "查驗台1": "1號檯",
    "查驗台4": "4號檯",
    "查驗台3": "3號檯",
    "自動通關": "自動通關",
    "發證": "補出櫃檯",
}

DEFAULT_TARGET_SLOTS = {
    ("05:00", "16:00"): 13,
    ("06:00", "17:00"): 13,
    ("07:00", "19:00"): 14,
    ("08:00", "20:00"): 14,
}


@dataclass(frozen=True)
class Employee:
    name: str
    shift_start: str
    shift_end: str
    skills: frozenset[str]
    dedicated_roles: frozenset[str]
    target_slots: int


@dataclass(frozen=True)
class SolverSettings:
    weight_last_hour_work: int = 50
    weight_group_fairness: int = 8
    weight_target_deviation: int = 3
    weight_same_hour_consistency: int = 12
    weight_single_slot_fragment: int = 18
    weight_early_late_equal_soft: int = 160
    weight_dedicated_assignment_bonus: int = 28
    weight_dedicated_miss_when_available: int = 85
    weight_single_dedicated_miss: int = 140
    weight_dedicated_frontload: int = 8
    weight_non_dedicated_when_ded_available: int = 28
    weight_dedicated_assignment_bonus_public: int = 44
    weight_dedicated_miss_when_available_public: int = 135
    weight_single_dedicated_miss_public: int = 220
    weight_dedicated_frontload_public: int = 14
    weight_non_dedicated_when_ded_available_public: int = 96
    dedicated_frontload_slots: int = 4
    weight_auto_gate_balance: int = 22
    weight_consecutive_2p5h: int = 10
    weight_consecutive_3h: int = 26
    weight_consecutive_3p5h: int = 60
    weight_shortage_slot: int = 100000
    auto_gate_max_slots: int = 6
    max_consecutive_work_slots: int = 7  # 3.5 hours hard limit for late shift
    early_max_consecutive_work_slots: int = 7  # 3.5 hours hard limit for early shift
    early_max_work_slots: int = 14  # 7.0 hours
    late_max_work_slots: int = 15   # 7.5 hours
    enforce_early_late_equal_hours: bool = False
    enforce_late_longer_than_early: bool = False
    min_late_minus_early_slots: int = 1  # at least 0.5 hour longer
    enforce_shift_work_caps: bool = False
    weight_shift_cap_excess: int = 30
    feasibility_mode: str = "hard"  # hard | allow_shortage
    max_time_sec: int = 30


@dataclass(frozen=True)
class ProblemData:
    employees: list[Employee]
    roles: list[str]
    time_labels: list[str]
    demand: dict[tuple[int, str], int]


@dataclass
class SolveResult:
    status: str
    feasible: bool
    mode_used: str
    assign: dict[tuple[int, int], str]
    work: dict[tuple[int, int], int]
    shortage: dict[tuple[int, str], int]
    on_duty: list[list[bool]]
    objective: float | None


@dataclass(frozen=True)
class DryRunStats:
    total_demand_slots: int
    total_on_duty_slots: int
    role_skill_gap_rows: pd.DataFrame


@dataclass(frozen=True)
class HardConstraintFlags:
    auto_gate_limit: bool = True
    consecutive_limit: bool = True
    equal_hours: bool = True
    late_longer_than_early: bool = True
    shift_work_caps: bool = True


@dataclass(frozen=True)
class FeasibilityAudit:
    feasible: bool
    issues_df: pd.DataFrame


@dataclass(frozen=True)
class UnsatDiagnosis:
    minimal_relax_sets: list[tuple[str, ...]]
    tested_cases: int
    any_relaxation_feasible: bool


def _normalize_time(value: Any, field_name: str) -> str:
    if pd.isna(value):
        raise ValueError(f"{field_name} 不可為空")
    s = str(value).strip()
    if not s:
        raise ValueError(f"{field_name} 不可為空")
    ts = pd.to_datetime(s, format="%H:%M", errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"{field_name} 時間格式錯誤: {s} (需 HH:MM)")
    return ts.strftime("%H:%M")


def _pick_col(columns: list[str], aliases: list[str]) -> str | None:
    cmap = {str(c).strip(): c for c in columns}
    for a in aliases:
        if a in cmap:
            return cmap[a]
    return None


def _canon_role(role_name: str) -> str:
    s = str(role_name).strip()
    return ROLE_ALIASES.get(s, s)


def _display_role(role_name: str) -> str:
    return ROLE_DISPLAY_NAMES.get(role_name, role_name)


def _normalize_shift_start(value: Any, field_name: str) -> str:
    if pd.isna(value):
        raise ValueError(f"{field_name} 不可為空")
    s = str(value).strip()
    if not s:
        raise ValueError(f"{field_name} 不可為空")

    try:
        f = float(s)
        if f.is_integer():
            h = int(f)
            if h in (5, 6, 7, 8):
                return f"{h:02d}:00"
    except Exception:
        pass

    t = _normalize_time(s, field_name)
    if t not in SHIFT_START_TO_END:
        raise ValueError(f"{field_name} 僅允許 05:00/06:00/07:00/08:00")
    return t


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    if str(value).strip() == "":
        return True
    return False


def _time_to_min(label: str) -> int:
    hh, mm = label.split(":")
    return int(hh) * 60 + int(mm)


def _is_true(v: Any) -> bool:
    if pd.isna(v):
        return False
    if isinstance(v, (int, float)):
        try:
            return float(v) > 0
        except Exception:
            return False
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "是"}


def _is_dedicated(v: Any) -> bool:
    if pd.isna(v):
        return False
    if isinstance(v, (int, float)):
        try:
            return int(float(v)) == 2
        except Exception:
            return False
    s = str(v).strip().lower()
    return s in {"2", "2.0", "專責", "dedicated", "d"}


def _is_auto_gate_role(role_name: str) -> bool:
    s = _canon_role(str(role_name).strip())
    return ("自動通關" in s) or (s == "E-Gate")


def _is_public_counter_role(role_name: str) -> bool:
    return _canon_role(str(role_name).strip()) == "公務台"


def _extract_skills(row: pd.Series, roles: list[str]) -> frozenset[str]:
    roles_set = set(roles)
    row_cols = [str(c).strip() for c in row.index]
    skills_col = _pick_col(row_cols, EMPLOYEE_SKILLS_ALIASES)
    if skills_col is not None and not pd.isna(row.get(skills_col, None)):
        raw = [x.strip() for x in str(row.get(skills_col, "")).split(",")]
        canon = [_canon_role(x) for x in raw if x]
        return frozenset([x for x in canon if x in roles_set])

    out = set()
    for col in row.index:
        c = str(col)
        if c.startswith("skill_"):
            role_name = c[len("skill_"):].strip()
        else:
            role_name = c
        role_name = _canon_role(role_name)
        if role_name in roles_set and _is_true(row.get(col, None)):
            out.add(role_name)
    return frozenset(out)


def _extract_dedicated_roles(row: pd.Series, roles: list[str]) -> frozenset[str]:
    roles_set = set(roles)
    row_cols = [str(c).strip() for c in row.index]
    skills_col = _pick_col(row_cols, EMPLOYEE_SKILLS_ALIASES)
    out = set()

    if skills_col is not None and not pd.isna(row.get(skills_col, None)):
        # Optional text syntax in skills cell: e.g. "公務台*,查驗台1(2)"
        for tok in [x.strip() for x in str(row.get(skills_col, "")).split(",") if x and str(x).strip()]:
            mark = tok
            dedicated = False
            if mark.endswith("*"):
                dedicated = True
                mark = mark[:-1]
            if mark.endswith("(2)"):
                dedicated = True
                mark = mark[:-3]
            role_name = _canon_role(mark.strip())
            if dedicated and role_name in roles_set:
                out.add(role_name)

    for col in row.index:
        c = str(col)
        if c.startswith("skill_"):
            role_name = c[len("skill_"):].strip()
        else:
            role_name = c
        role_name = _canon_role(role_name)
        if role_name in roles_set and _is_dedicated(row.get(col, None)):
            out.add(role_name)
    return frozenset(out)


def _calc_target_slots(shift_start: str, shift_end: str, value: Any) -> int:
    if value is None or pd.isna(value):
        return DEFAULT_TARGET_SLOTS[(shift_start, shift_end)]
    minutes = int(float(value))
    return max(0, minutes // 30)


def read_input(input_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    p = Path(input_path)
    if not p.exists():
        raise ValueError(f"找不到輸入檔案: {input_path}")
    xls = pd.ExcelFile(input_path)
    sheets = set(xls.sheet_names)
    for req in ("Employees", "Demand"):
        if req not in sheets:
            raise ValueError(f"缺少工作表: {req}")
    emp_df = pd.read_excel(input_path, sheet_name="Employees")
    dem_df = pd.read_excel(input_path, sheet_name="Demand")
    return emp_df, dem_df


def validate_input(emp_df: pd.DataFrame, dem_df: pd.DataFrame) -> None:
    emp_cols = [str(c).strip() for c in emp_df.columns]
    dem_cols = [str(c).strip() for c in dem_df.columns]

    name_col = _pick_col(emp_cols, EMPLOYEE_NAME_ALIASES)
    shift_start_col = _pick_col(emp_cols, EMPLOYEE_SHIFT_START_ALIASES)
    shift_end_col = _pick_col(emp_cols, EMPLOYEE_SHIFT_END_ALIASES)
    time_col = _pick_col(dem_cols, DEMAND_TIME_ALIASES)

    if name_col is None:
        raise ValueError(f"Employees 缺少欄位: {EMPLOYEE_NAME_ALIASES}")
    if shift_start_col is None:
        raise ValueError(f"Employees 缺少欄位: {EMPLOYEE_SHIFT_START_ALIASES}")
    if time_col is None:
        raise ValueError(f"Demand 缺少欄位: {DEMAND_TIME_ALIASES}")

    roles_raw = [str(c).strip() for c in dem_df.columns if str(c).strip() != time_col]
    roles = [_canon_role(r) for r in roles_raw]
    if not roles:
        raise ValueError("Demand 至少需要 1 個 role 欄位")
    if len(roles) != len(set(roles)):
        raise ValueError("Demand role 欄位名稱（別名轉換後）不可重複")

    # Validate time sequence and full range
    time_labels = [_normalize_time(v, "Demand.time") for v in dem_df[time_col].tolist()]
    if len(time_labels) != len(set(time_labels)):
        raise ValueError("Demand.time 有重複")

    expected = []
    cur = _time_to_min("05:00")
    end = _time_to_min("19:30")
    while cur <= end:
        hh = cur // 60
        mm = cur % 60
        expected.append(f"{hh:02d}:{mm:02d}")
        cur += 30

    if time_labels != expected:
        raise ValueError(
            "Demand.time 必須完整且按序覆蓋 05:00,05:30,...,19:30"
        )

    # Demand values
    for t_idx, row in dem_df.iterrows():
        for r_raw in roles_raw:
            v = row.get(r_raw, 0)
            if pd.isna(v):
                v = 0
            n = int(float(v))
            if n < 0:
                raise ValueError(f"Demand 負值: row={t_idx+2}, role={r_raw}, value={n}")

    # Employee rows
    names = set()
    for ridx, row in emp_df.iterrows():
        if row.isna().all():
            continue
        name = str(row.get(name_col, "")).strip()
        if not name:
            raise ValueError(f"Employees 第 {ridx+2} 列 name 為空")
        if name in names:
            raise ValueError(f"Employees name 重複: {name}")
        names.add(name)

        shift_start_val = row.get(shift_start_col, None)
        if _is_blank(shift_start_val):
            # 空白表示該員工本次休假或不納入排程
            continue

        s = _normalize_shift_start(shift_start_val, f"Employees[{name}].上勤時間")
        if shift_end_col is not None and not pd.isna(row.get(shift_end_col, None)):
            e = _normalize_time(row.get(shift_end_col), f"Employees[{name}].下勤時間")
        else:
            e = SHIFT_START_TO_END[s]
        if (s, e) not in VALID_WINDOWS:
            raise ValueError(f"Employees[{name}] 班段不合法: {s}-{e}")


def build_problem_data(emp_df: pd.DataFrame, dem_df: pd.DataFrame) -> ProblemData:
    emp_cols = [str(c).strip() for c in emp_df.columns]
    dem_cols = [str(c).strip() for c in dem_df.columns]
    name_col = _pick_col(emp_cols, EMPLOYEE_NAME_ALIASES)
    shift_start_col = _pick_col(emp_cols, EMPLOYEE_SHIFT_START_ALIASES)
    shift_end_col = _pick_col(emp_cols, EMPLOYEE_SHIFT_END_ALIASES)
    target_col = _pick_col(emp_cols, EMPLOYEE_TARGET_MIN_ALIASES)
    time_col = _pick_col(dem_cols, DEMAND_TIME_ALIASES)
    if name_col is None or shift_start_col is None or time_col is None:
        raise ValueError("請先通過 validate_input()")

    roles_raw = [str(c).strip() for c in dem_df.columns if str(c).strip() != time_col]
    roles = [_canon_role(r) for r in roles_raw]
    time_labels = [_normalize_time(v, "Demand.time") for v in dem_df[time_col].tolist()]

    demand: dict[tuple[int, str], int] = {}
    for t_idx, (_, row) in enumerate(dem_df.iterrows()):
        for r_raw, r in zip(roles_raw, roles):
            v = row.get(r_raw, 0)
            if pd.isna(v):
                v = 0
            demand[(t_idx, r)] = int(float(v))

    employees = []
    for _, row in emp_df.iterrows():
        if row.isna().all():
            continue
        name = str(row.get(name_col, "")).strip()
        if not name:
            continue
        shift_start_val = row.get(shift_start_col, None)
        if _is_blank(shift_start_val):
            continue
        s = _normalize_shift_start(shift_start_val, "Employees.shift_start")
        if shift_end_col is not None and not pd.isna(row.get(shift_end_col, None)):
            e = _normalize_time(row.get(shift_end_col), "Employees.shift_end")
        else:
            e = SHIFT_START_TO_END[s]
        skills = _extract_skills(row, roles)
        dedicated_roles = _extract_dedicated_roles(row, roles)
        if dedicated_roles:
            skills = frozenset(set(skills) | set(dedicated_roles))
        target_slots = _calc_target_slots(s, e, row.get(target_col, None) if target_col is not None else None)
        employees.append(
            Employee(
                name=name,
                shift_start=s,
                shift_end=e,
                skills=skills,
                dedicated_roles=dedicated_roles,
                target_slots=target_slots,
            )
        )

    return ProblemData(employees=employees, roles=roles, time_labels=time_labels, demand=demand)


def dry_run_stats(data: ProblemData) -> DryRunStats:
    total_demand_slots = sum(data.demand.values())
    total_on_duty_slots = 0

    on_duty_map = _build_on_duty_map(data)
    for e in range(len(data.employees)):
        total_on_duty_slots += sum(1 for t in range(len(data.time_labels)) if on_duty_map[e][t])

    rows = []
    for t, tl in enumerate(data.time_labels):
        for r in data.roles:
            req = data.demand[(t, r)]
            skilled_on_duty = 0
            for e_idx, emp in enumerate(data.employees):
                if on_duty_map[e_idx][t] and r in emp.skills:
                    skilled_on_duty += 1
            rows.append(
                {
                    "time": tl,
                    "role": r,
                    "demand": req,
                    "skilled_on_duty": skilled_on_duty,
                    "skill_gap": max(0, req - skilled_on_duty),
                }
            )

    gap_df = pd.DataFrame(rows)
    gap_df = gap_df[gap_df["skill_gap"] > 0].sort_values(["skill_gap", "time", "role"], ascending=[False, True, True])

    return DryRunStats(
        total_demand_slots=total_demand_slots,
        total_on_duty_slots=total_on_duty_slots,
        role_skill_gap_rows=gap_df,
    )


def _shift_hour(shift_start: str) -> int:
    try:
        return int(str(shift_start).split(":")[0])
    except Exception:
        return -1


def _group_member_indices(data: ProblemData) -> tuple[list[int], list[int]]:
    early_members = []
    late_members = []
    for e_idx, emp in enumerate(data.employees):
        h = _shift_hour(emp.shift_start)
        if h in (5, 6):
            early_members.append(e_idx)
        elif h in (7, 8):
            late_members.append(e_idx)
    return early_members, late_members


def feasibility_audit(data: ProblemData, settings: SolverSettings) -> FeasibilityAudit:
    """
    Pre-solve necessary-condition checks.
    This is intentionally conservative: any reported issue is a hard conflict.
    """
    issues: list[dict[str, Any]] = []
    on_duty = _build_on_duty_map(data)
    num_t = len(data.time_labels)

    # A) per-time/role skill capacity and total manpower checks
    for t in range(num_t):
        tl = data.time_labels[t]
        total_req_t = 0
        for r in data.roles:
            req = data.demand[(t, r)]
            total_req_t += req
            if req <= 0:
                continue
            skilled_on_duty = sum(1 for e_idx, emp in enumerate(data.employees) if on_duty[e_idx][t] and r in emp.skills)
            if skilled_on_duty < req:
                issues.append(
                    {
                        "check": "time_role_skill_capacity",
                        "time": tl,
                        "role": r,
                        "detail": f"skilled_on_duty={skilled_on_duty} < demand={req}",
                    }
                )
        on_duty_cnt = sum(1 for e_idx in range(len(data.employees)) if on_duty[e_idx][t])
        if on_duty_cnt < total_req_t:
            issues.append(
                {
                    "check": "time_total_manpower",
                    "time": tl,
                    "role": "ALL",
                    "detail": f"on_duty={on_duty_cnt} < total_demand={total_req_t}",
                }
            )

    # B) total supply + divisibility checks under equal-hours and late>early hard rules
    total_demand_slots = int(sum(data.demand.values()))
    early_members, late_members = _group_member_indices(data)
    n_early = len(early_members)
    n_late = len(late_members)

    # Per-person work caps for auditor (hard caps only).
    on_duty_slots = [sum(1 for t in range(num_t) if on_duty[e_idx][t]) for e_idx in range(len(data.employees))]
    per_cap = []
    for e_idx, emp in enumerate(data.employees):
        h = _shift_hour(emp.shift_start)
        cap = on_duty_slots[e_idx]
        if settings.enforce_shift_work_caps:
            if h in (5, 6):
                cap = min(cap, settings.early_max_work_slots)
            elif h in (7, 8):
                cap = min(cap, settings.late_max_work_slots)
        per_cap.append(cap)

    if settings.enforce_early_late_equal_hours and (n_early > 0 or n_late > 0):
        early_cap = min([per_cap[e_idx] for e_idx in early_members], default=0)
        late_cap = min([per_cap[e_idx] for e_idx in late_members], default=0)
        early_vals = [0] if n_early == 0 else list(range(0, early_cap + 1))
        late_vals = [0] if n_late == 0 else list(range(0, late_cap + 1))
        min_gap = max(1, int(settings.min_late_minus_early_slots))

        feasible_exact_pairs = []
        max_supply = 0
        for ev in early_vals:
            for lv in late_vals:
                if settings.enforce_late_longer_than_early and n_early > 0 and n_late > 0 and lv < ev + min_gap:
                    continue
                supply = n_early * ev + n_late * lv
                if supply > max_supply:
                    max_supply = supply
                if supply == total_demand_slots:
                    feasible_exact_pairs.append((ev, lv))

        if max_supply < total_demand_slots:
            issues.append(
                {
                    "check": "total_supply_upper_bound",
                    "time": "ALL",
                    "role": "ALL",
                    "detail": f"max_supply={max_supply} < total_demand={total_demand_slots}",
                }
            )

        if not feasible_exact_pairs:
            issues.append(
                {
                    "check": "group_divisibility_equal_hours",
                    "time": "ALL",
                    "role": "ALL",
                    "detail": (
                        f"no integer (early_slots, late_slots) satisfies "
                        f"nE*e+nL*l=total_demand with nE={n_early}, nL={n_late}, "
                        f"e<= {early_cap}, l<= {late_cap}, "
                        f"late>early={settings.enforce_late_longer_than_early}"
                    ),
                }
            )
    else:
        max_supply = sum(per_cap)
        if max_supply < total_demand_slots:
            issues.append(
                {
                    "check": "total_supply_upper_bound",
                    "time": "ALL",
                    "role": "ALL",
                    "detail": f"max_supply={max_supply} < total_demand={total_demand_slots}",
                }
            )

    # C) auto-gate global capacity (necessary condition)
    auto_roles = [r for r in data.roles if _is_auto_gate_role(r)]
    if auto_roles:
        total_auto_demand = 0
        for t in range(num_t):
            for r in auto_roles:
                total_auto_demand += int(data.demand[(t, r)])
        auto_capacity = 0
        for e_idx, emp in enumerate(data.employees):
            if not any(_is_auto_gate_role(sk) for sk in emp.skills):
                continue
            auto_capacity += on_duty_slots[e_idx]
        if auto_capacity < total_auto_demand:
            issues.append(
                {
                    "check": "auto_gate_total_capacity",
                    "time": "ALL",
                    "role": "自動通關",
                    "detail": f"auto_capacity={auto_capacity} < auto_demand={total_auto_demand}",
                }
            )

    if not issues:
        return FeasibilityAudit(feasible=True, issues_df=pd.DataFrame(columns=["check", "time", "role", "detail"]))
    return FeasibilityAudit(feasible=False, issues_df=pd.DataFrame(issues))


def _build_on_duty_map(data: ProblemData) -> list[list[bool]]:
    on_duty = [[False] * len(data.time_labels) for _ in data.employees]
    for e_idx, emp in enumerate(data.employees):
        s_min = _time_to_min(emp.shift_start)
        e_min = _time_to_min(emp.shift_end)
        for t, tl in enumerate(data.time_labels):
            tm = _time_to_min(tl)
            on_duty[e_idx][t] = s_min <= tm < e_min
    return on_duty


def _default_hard_flags(settings: SolverSettings) -> HardConstraintFlags:
    return HardConstraintFlags(
        auto_gate_limit=True,
        consecutive_limit=True,
        equal_hours=bool(settings.enforce_early_late_equal_hours),
        late_longer_than_early=bool(settings.enforce_late_longer_than_early),
        shift_work_caps=bool(settings.enforce_shift_work_caps),
    )


def solve(
    data: ProblemData,
    settings: SolverSettings,
    mode_override: str | None = None,
    *,
    optimize_soft: bool = True,
    hint_result: SolveResult | None = None,
    hard_flags: HardConstraintFlags | None = None,
    time_sec_override: int | None = None,
    locked_role_assignments: dict[tuple[int, int], str] | None = None,
    locked_work_state: dict[tuple[int, int], int] | None = None,
) -> SolveResult:
    mode = mode_override or settings.feasibility_mode
    strict = (mode == "hard")
    flags = hard_flags or _default_hard_flags(settings)

    model = cp_model.CpModel()
    num_e = len(data.employees)
    num_t = len(data.time_labels)
    on_duty = _build_on_duty_map(data)
    locked_role_assignments = dict(locked_role_assignments or {})
    locked_work_state = {k: int(v) for k, v in (locked_work_state or {}).items()}

    seats = {r: list(range(max(data.demand[(t, r)] for t in range(num_t)))) for r in data.roles}

    x: dict[tuple[int, int, str, int], cp_model.IntVar] = {}
    for e_idx, emp in enumerate(data.employees):
        for t in range(num_t):
            if not on_duty[e_idx][t]:
                continue
            for r in data.roles:
                if r not in emp.skills:
                    continue
                for s in seats[r]:
                    x[(e_idx, t, r, s)] = model.NewBoolVar(f"x_e{e_idx}_t{t}_r{r}_s{s}")

    work: dict[tuple[int, int], cp_model.IntVar] = {}
    for e_idx in range(num_e):
        for t in range(num_t):
            w = model.NewBoolVar(f"work_e{e_idx}_t{t}")
            work[(e_idx, t)] = w
            vars_here = [
                x[(e_idx, t, r, s)]
                for r in data.roles
                for s in seats[r]
                if (e_idx, t, r, s) in x
            ]
            if not on_duty[e_idx][t] or not vars_here:
                model.Add(w == 0)
            else:
                model.Add(sum(vars_here) == w)

    # y[e,t,r] = employee e works role r at time t (seat-agnostic).
    y_role: dict[tuple[int, int, str], cp_model.IntVar] = {}
    for e_idx in range(num_e):
        for t in range(num_t):
            for r in data.roles:
                y = model.NewBoolVar(f"y_e{e_idx}_t{t}_r{r}")
                seat_vars = [
                    x[(e_idx, t, r, s)]
                    for s in seats[r]
                    if (e_idx, t, r, s) in x
                ]
                if seat_vars:
                    model.Add(y == sum(seat_vars))
                else:
                    model.Add(y == 0)
                y_role[(e_idx, t, r)] = y

    # external lock constraints (for interactive/local re-optimize)
    for (e_idx, t), val in locked_work_state.items():
        if 0 <= e_idx < num_e and 0 <= t < num_t:
            model.Add(work[(e_idx, t)] == (1 if int(val) else 0))
    for (e_idx, t), role in locked_role_assignments.items():
        if 0 <= e_idx < num_e and 0 <= t < num_t and role in data.roles:
            model.Add(y_role[(e_idx, t, role)] == 1)

    # per employee per time <=1
    for e_idx in range(num_e):
        for t in range(num_t):
            vars_here = [
                x[(e_idx, t, r, s)]
                for r in data.roles
                for s in seats[r]
                if (e_idx, t, r, s) in x
            ]
            if vars_here:
                model.Add(sum(vars_here) <= 1)

    # per seat per time <=1 and coverage
    shortage: dict[tuple[int, str], cp_model.IntVar] = {}
    seat_used_map: dict[tuple[int, str, int], cp_model.IntVar] = {}
    for t in range(num_t):
        for r in data.roles:
            seat_used_vars = []
            for s in seats[r]:
                assign_vars = [x[(e, t, r, s)] for e in range(num_e) if (e, t, r, s) in x]
                if assign_vars:
                    used = model.NewBoolVar(f"seat_used_t{t}_r{r}_s{s}")
                    model.Add(sum(assign_vars) == used)
                    seat_used_vars.append(used)
                    seat_used_map[(t, r, s)] = used
                else:
                    zero = model.NewConstant(0)
                    seat_used_vars.append(zero)
                    seat_used_map[(t, r, s)] = zero

            assigned_cnt = sum(seat_used_vars)
            req = data.demand[(t, r)]
            if strict:
                model.Add(assigned_cnt == req)
            else:
                sh = model.NewIntVar(0, req, f"short_t{t}_r{r}")
                shortage[(t, r)] = sh
                model.Add(assigned_cnt + sh == req)

    # Symmetry breaking for same-role seats: fill lower seats first.
    for t in range(num_t):
        for r in data.roles:
            for s in range(1, len(seats[r])):
                model.Add(seat_used_map[(t, r, s - 1)] >= seat_used_map[(t, r, s)])

    # auto-gate: max 4 consecutive slots (2 hours) — was previously 1 slot (too strict).
    auto_roles = [r for r in data.roles if _is_auto_gate_role(r)]
    if auto_roles and flags.auto_gate_limit:
        max_auto_consec = 4
        for e_idx in range(num_e):
            for st in range(0, num_t - max_auto_consec):
                consec_window = [y_role[(e_idx, t, r)] for t in range(st, st + max_auto_consec + 1) for r in auto_roles]
                model.Add(sum(consec_window) <= max_auto_consec)

    # max consecutive work slots (hard):
    # - early shift: <= 2.5h (5 slots)
    # - late shift: <= 3.0h (6 slots)
    if flags.consecutive_limit:
        for e_idx, emp in enumerate(data.employees):
            try:
                start_h = int(str(emp.shift_start).split(":")[0])
            except Exception:
                start_h = -1
            max_c = settings.early_max_consecutive_work_slots if start_h in (5, 6) else settings.max_consecutive_work_slots
            max_c = max(1, int(max_c))
            for st in range(0, num_t - max_c):
                model.Add(sum(work[(e_idx, t)] for t in range(st, st + max_c + 1)) <= max_c)

    # objective terms
    penalties: list[cp_model.LinearExpr] = []

    # A) last hour penalty
    if optimize_soft:
        for e_idx, emp in enumerate(data.employees):
            end_min = _time_to_min(emp.shift_end)
            terms = []
            for t, tl in enumerate(data.time_labels):
                tm = _time_to_min(tl)
                if end_min - 60 <= tm < end_min:
                    terms.append(work[(e_idx, t)])
            if terms:
                penalties.append(settings.weight_last_hour_work * sum(terms))

    # total slots per employee
    total_slots_vars = []
    for e_idx in range(num_e):
        cap = sum(1 for t in range(num_t) if on_duty[e_idx][t])
        var = model.NewIntVar(0, cap, f"total_slots_e{e_idx}")
        model.Add(var == sum(work[(e_idx, t)] for t in range(num_t)))
        # Shift-group workload cap:
        # early (05/06) target <= 7.0h (14 slots), late (07/08) target <= 7.5h (15 slots).
        # Can be enforced as hard or soft by settings.
        try:
            start_h = int(str(data.employees[e_idx].shift_start).split(":")[0])
        except Exception:
            start_h = -1
        group_cap = None
        if start_h in (5, 6):
            group_cap = settings.early_max_work_slots
        elif start_h in (7, 8):
            group_cap = settings.late_max_work_slots
        if group_cap is not None:
            if settings.enforce_shift_work_caps and flags.shift_work_caps:
                model.Add(var <= group_cap)
            elif optimize_soft:
                excess = model.NewIntVar(0, cap, f"cap_excess_e{e_idx}")
                model.Add(excess >= var - group_cap)
                if settings.weight_shift_cap_excess > 0:
                    penalties.append(settings.weight_shift_cap_excess * excess)
        total_slots_vars.append(var)

    # B) fairness within same window
    if optimize_soft:
        groups: dict[tuple[str, str], list[int]] = {}
        for e_idx, emp in enumerate(data.employees):
            groups.setdefault((emp.shift_start, emp.shift_end), []).append(e_idx)

        for key, members in groups.items():
            if len(members) <= 1:
                continue
            gmax = model.NewIntVar(0, num_t, f"gmax_{key[0]}_{key[1]}")
            gmin = model.NewIntVar(0, num_t, f"gmin_{key[0]}_{key[1]}")
            model.AddMaxEquality(gmax, [total_slots_vars[m] for m in members])
            model.AddMinEquality(gmin, [total_slots_vars[m] for m in members])
            diff = model.NewIntVar(0, num_t, f"gdiff_{key[0]}_{key[1]}")
            model.Add(diff == gmax - gmin)
            penalties.append(settings.weight_group_fairness * diff)

    early_members = []
    late_members = []
    for e_idx, emp in enumerate(data.employees):
        try:
            h = int(str(emp.shift_start).split(":")[0])
        except Exception:
            h = -1
        if h in (5, 6):
            early_members.append(e_idx)
        elif h in (7, 8):
            late_members.append(e_idx)

    # B2-soft) Early/Late group equal-hours as strong soft preference.
    if optimize_soft and settings.weight_early_late_equal_soft > 0:
        for gname, members in [("early", early_members), ("late", late_members)]:
            if len(members) <= 1:
                continue
            gmax = model.NewIntVar(0, num_t, f"eq_soft_gmax_{gname}")
            gmin = model.NewIntVar(0, num_t, f"eq_soft_gmin_{gname}")
            model.AddMaxEquality(gmax, [total_slots_vars[m] for m in members])
            model.AddMinEquality(gmin, [total_slots_vars[m] for m in members])
            gdiff = model.NewIntVar(0, num_t, f"eq_soft_gdiff_{gname}")
            model.Add(gdiff == gmax - gmin)
            penalties.append(settings.weight_early_late_equal_soft * gdiff)

    # B2-hard) hard equal hours inside Early(05/06) and Late(07/08) groups.
    early_anchor_var = None
    late_anchor_var = None
    if settings.enforce_early_late_equal_hours and flags.equal_hours:
        for gname, members in [("early", early_members), ("late", late_members)]:
            if len(members) <= 1:
                continue
            anchor = total_slots_vars[members[0]]
            if gname == "early":
                early_anchor_var = anchor
            else:
                late_anchor_var = anchor
            for m in members[1:]:
                model.Add(total_slots_vars[m] == anchor)
        if len(early_members) == 1:
            early_anchor_var = total_slots_vars[early_members[0]]
        if len(late_members) == 1:
            late_anchor_var = total_slots_vars[late_members[0]]

    # B3) late shift average must be longer than early shift average (hard).
    if (
        settings.enforce_late_longer_than_early
        and flags.late_longer_than_early
        and early_members
        and late_members
    ):
        min_gap = max(1, int(settings.min_late_minus_early_slots))
        if (
            settings.enforce_early_late_equal_hours
            and flags.equal_hours
            and early_anchor_var is not None
            and late_anchor_var is not None
        ):
            model.Add(late_anchor_var >= early_anchor_var + min_gap)
        else:
            early_sum = sum(total_slots_vars[m] for m in early_members)
            late_sum = sum(total_slots_vars[m] for m in late_members)
            # avg_late >= avg_early + min_gap
            model.Add(
                len(early_members) * late_sum
                >= len(late_members) * early_sum + min_gap * len(early_members) * len(late_members)
            )

    # C) target deviation
    if not strict and shortage:
        penalties.append(settings.weight_shortage_slot * sum(shortage.values()))

    if optimize_soft:
        # C) target deviation
        for e_idx, emp in enumerate(data.employees):
            dev = model.NewIntVar(0, num_t, f"dev_e{e_idx}")
            model.AddAbsEquality(dev, total_slots_vars[e_idx] - emp.target_slots)
            penalties.append(settings.weight_target_deviation * dev)

        # D) 同一小時兩個半小時盡量同人同崗（減少 30 分鐘碎裂）
        same_hour_terms = []
        for t in range(0, num_t - 1, 2):
            for e_idx in range(num_e):
                for r in data.roles:
                    a = y_role[(e_idx, t, r)]
                    b = y_role[(e_idx, t + 1, r)]
                    diff = model.NewBoolVar(f"hrdiff_e{e_idx}_t{t}_r{r}")
                    model.Add(a - b <= diff)
                    model.Add(b - a <= diff)
                    same_hour_terms.append(diff)
        if same_hour_terms:
            penalties.append(settings.weight_same_hour_consistency * sum(same_hour_terms))

        # E) 單一 30 分鐘孤立片段懲罰（鼓勵至少連續 1 小時）
        singleton_terms = []
        for e_idx in range(num_e):
            for t in range(1, num_t - 1):
                sng = model.NewBoolVar(f"singleton_e{e_idx}_t{t}")
                model.Add(sng <= work[(e_idx, t)])
                model.Add(sng <= 1 - work[(e_idx, t - 1)])
                model.Add(sng <= 1 - work[(e_idx, t + 1)])
                model.Add(sng >= work[(e_idx, t)] - work[(e_idx, t - 1)] - work[(e_idx, t + 1)])
                singleton_terms.append(sng)
        if singleton_terms:
            penalties.append(settings.weight_single_slot_fragment * sum(singleton_terms))

        # E2) Long consecutive stretches soft penalty.
        # Early shift: soft escalation starts at 2.5h (5 slots), harder at 3h (6 slots), heaviest at 3.5h (7 slots).
        # Late shift: soft at 2.5h and 3h (hard limit is 3h).
        run5_terms = []
        if settings.weight_consecutive_2p5h > 0:
            for e_idx in range(num_e):
                for st in range(0, num_t - 4):
                    b5 = model.NewBoolVar(f"run5_e{e_idx}_st{st}")
                    window = [work[(e_idx, st + i)] for i in range(5)]
                    for w in window:
                        model.Add(b5 <= w)
                    model.Add(b5 >= sum(window) - 4)
                    run5_terms.append(b5)
            if run5_terms:
                penalties.append(settings.weight_consecutive_2p5h * sum(run5_terms))

        run6_terms = []
        if settings.weight_consecutive_3h > 0:
            for e_idx in range(num_e):
                for st in range(0, num_t - 5):
                    b6 = model.NewBoolVar(f"run6_e{e_idx}_st{st}")
                    window = [work[(e_idx, st + i)] for i in range(6)]
                    for w in window:
                        model.Add(b6 <= w)
                    model.Add(b6 >= sum(window) - 5)
                    run6_terms.append(b6)
            if run6_terms:
                penalties.append(settings.weight_consecutive_3h * sum(run6_terms))

        # 3.5h run (7 slots) soft penalty — all shifts (hard limit is also 3.5h for both).
        run7_terms = []
        if settings.weight_consecutive_3p5h > 0:
            for e_idx in range(num_e):
                for st in range(0, num_t - 6):
                    b7 = model.NewBoolVar(f"run7_e{e_idx}_st{st}")
                    window = [work[(e_idx, st + i)] for i in range(7)]
                    for w in window:
                        model.Add(b7 <= w)
                    model.Add(b7 >= sum(window) - 6)
                    run7_terms.append(b7)
            if run7_terms:
                penalties.append(settings.weight_consecutive_3p5h * sum(run7_terms))

        # F) 專責偏好:
        # - 專責被排到其專責崗位時給獎勵（降低目標值）
        # - 若該時段有專責在班且該崗位有需求，盡量由專責上崗（違反則吃高懲罰）
        def _w_bonus(role: str) -> int:
            if _is_public_counter_role(role):
                return int(settings.weight_dedicated_assignment_bonus_public)
            return int(settings.weight_dedicated_assignment_bonus)

        def _w_miss(role: str) -> int:
            if _is_public_counter_role(role):
                return int(settings.weight_dedicated_miss_when_available_public)
            return int(settings.weight_dedicated_miss_when_available)

        def _w_single_miss(role: str) -> int:
            if _is_public_counter_role(role):
                return int(settings.weight_single_dedicated_miss_public)
            return int(settings.weight_single_dedicated_miss)

        def _w_frontload(role: str) -> int:
            if _is_public_counter_role(role):
                return int(settings.weight_dedicated_frontload_public)
            return int(settings.weight_dedicated_frontload)

        def _w_non_ded(role: str) -> int:
            if _is_public_counter_role(role):
                return int(settings.weight_non_dedicated_when_ded_available_public)
            return int(settings.weight_non_dedicated_when_ded_available)

        for e_idx, emp in enumerate(data.employees):
            if not emp.dedicated_roles:
                continue
            for t in range(num_t):
                if not on_duty[e_idx][t]:
                    continue
                for r in emp.dedicated_roles:
                    if r in data.roles:
                        wb = _w_bonus(r)
                        if wb > 0:
                            penalties.append(-wb * y_role[(e_idx, t, r)])

        for t in range(num_t):
            for r in data.roles:
                req = data.demand[(t, r)]
                if req <= 0:
                    continue
                dedicated_on_duty = [
                    e_idx
                    for e_idx, emp in enumerate(data.employees)
                    if (r in emp.dedicated_roles) and on_duty[e_idx][t]
                ]
                if not dedicated_on_duty:
                    continue
                dedicated_assigned = sum(y_role[(e_idx, t, r)] for e_idx in dedicated_on_duty)
                miss = model.NewBoolVar(f"ded_miss_t{t}_r{r}")
                model.Add(dedicated_assigned == 0).OnlyEnforceIf(miss)
                model.Add(dedicated_assigned >= 1).OnlyEnforceIf(miss.Not())
                wm = _w_miss(r)
                if wm > 0:
                    penalties.append(wm * miss)

                # If only one dedicated is available at this time, strongly prefer assigning that person.
                ws = _w_single_miss(r)
                if len(dedicated_on_duty) == 1 and ws > 0:
                    only_e = dedicated_on_duty[0]
                    miss_single = model.NewBoolVar(f"ded_single_miss_e{only_e}_t{t}_r{r}")
                    model.Add(y_role[(only_e, t, r)] == 0).OnlyEnforceIf(miss_single)
                    model.Add(y_role[(only_e, t, r)] == 1).OnlyEnforceIf(miss_single.Not())
                    penalties.append(ws * miss_single)

                # If dedicated is available, assigning non-dedicated workers to this role is discouraged.
                wnd = _w_non_ded(r)
                if wnd > 0:
                    ded_set = set(dedicated_on_duty)
                    for e_idx, emp in enumerate(data.employees):
                        if e_idx in ded_set:
                            continue
                        if not on_duty[e_idx][t]:
                            continue
                        if r not in emp.skills:
                            continue
                        penalties.append(wnd * y_role[(e_idx, t, r)])

        # Front-load dedicated assignment: if dedicated employee is on duty and role has demand,
        # prefer assigning this dedicated role earlier in their shift.
        if settings.dedicated_frontload_slots > 0:
            for e_idx, emp in enumerate(data.employees):
                if not emp.dedicated_roles:
                    continue
                on_slots = [t for t in range(num_t) if on_duty[e_idx][t]]
                if not on_slots:
                    continue
                first_t = on_slots[0]
                for r in emp.dedicated_roles:
                    if r not in data.roles:
                        continue
                    for t in on_slots:
                        if t - first_t >= settings.dedicated_frontload_slots:
                            break
                        if data.demand[(t, r)] <= 0:
                            continue
                        wf = _w_frontload(r)
                        if wf <= 0:
                            continue
                        bonus = wf * (settings.dedicated_frontload_slots - (t - first_t))
                        penalties.append(-bonus * y_role[(e_idx, t, r)])

        # Auto-gate load balance: equalize auto slots within Early/Late groups among auto-skilled employees.
        auto_slots_vars: dict[int, cp_model.IntVar] = {}
        if auto_roles:
            for e_idx, emp in enumerate(data.employees):
                cap = sum(1 for t in range(num_t) if on_duty[e_idx][t])
                av = model.NewIntVar(0, cap, f"auto_slots_e{e_idx}")
                terms = []
                for t in range(num_t):
                    for r in auto_roles:
                        terms.append(y_role[(e_idx, t, r)])
                model.Add(av == (sum(terms) if terms else 0))
                auto_slots_vars[e_idx] = av

            if settings.weight_auto_gate_balance > 0:
                for group_name in ("early", "late"):
                    members = []
                    for e_idx, emp in enumerate(data.employees):
                        if not any(_is_auto_gate_role(sk) for sk in emp.skills):
                            continue
                        try:
                            h = int(str(emp.shift_start).split(":")[0])
                        except Exception:
                            h = -1
                        if (group_name == "early" and h in (5, 6)) or (group_name == "late" and h in (7, 8)):
                            members.append(e_idx)
                    if len(members) <= 1:
                        continue
                    gmax = model.NewIntVar(0, num_t, f"auto_gmax_{group_name}")
                    gmin = model.NewIntVar(0, num_t, f"auto_gmin_{group_name}")
                    model.AddMaxEquality(gmax, [auto_slots_vars[m] for m in members])
                    model.AddMinEquality(gmin, [auto_slots_vars[m] for m in members])
                    gdiff = model.NewIntVar(0, num_t, f"auto_gdiff_{group_name}")
                    model.Add(gdiff == gmax - gmin)
                    penalties.append(settings.weight_auto_gate_balance * gdiff)

    if hint_result is not None and hint_result.feasible:
        for e_idx in range(num_e):
            for t in range(num_t):
                hv = int(hint_result.work.get((e_idx, t), 0))
                model.AddHint(work[(e_idx, t)], hv)
                hinted_role = hint_result.assign.get((e_idx, t), None)
                for r in data.roles:
                    h = 1 if hinted_role == r else 0
                    model.AddHint(y_role[(e_idx, t, r)], h)
        if shortage:
            for key, var in shortage.items():
                model.AddHint(var, int(hint_result.shortage.get(key, 0)))

    if penalties:
        model.Minimize(sum(penalties))
    else:
        model.Minimize(0)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = max(1, int(time_sec_override if time_sec_override is not None else settings.max_time_sec))
    solver.parameters.num_search_workers = 8
    status_code = solver.Solve(model)
    status = solver.StatusName(status_code)

    feasible = status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE)
    if not feasible:
        return SolveResult(
            status=status,
            feasible=False,
            mode_used=mode,
            assign={},
            work={},
            shortage={},
            on_duty=on_duty,
            objective=None,
        )

    assign: dict[tuple[int, int], str] = {}
    work_out: dict[tuple[int, int], int] = {}
    for e_idx in range(num_e):
        for t in range(num_t):
            work_out[(e_idx, t)] = int(solver.Value(work[(e_idx, t)]))
            chosen = None
            for r in data.roles:
                for s in seats[r]:
                    k = (e_idx, t, r, s)
                    if k in x and solver.Value(x[k]) == 1:
                        chosen = r
                        break
                if chosen is not None:
                    break
            if chosen is not None:
                assign[(e_idx, t)] = chosen

    shortage_out = {(t, r): int(solver.Value(v)) for (t, r), v in shortage.items()}
    return SolveResult(
        status=status,
        feasible=True,
        mode_used=mode,
        assign=assign,
        work=work_out,
        shortage=shortage_out,
        on_duty=on_duty,
        objective=float(solver.ObjectiveValue()),
    )


def longest_consecutive_ones(bits: list[int]) -> int:
    best = 0
    cur = 0
    for b in bits:
        if b:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return best


def diagnose_unsat_constraints(
    data: ProblemData,
    settings: SolverSettings,
    *,
    time_limit_per_case_sec: int = 4,
    locked_role_assignments: dict[tuple[int, int], str] | None = None,
    locked_work_state: dict[tuple[int, int], int] | None = None,
) -> UnsatDiagnosis:
    """
    Practical UNSAT diagnosis via constraint-family toggles.
    Finds minimal relaxation sets that make hard model feasible.
    """
    families: list[str] = []
    if True:
        families.append("auto_gate_limit")
    if True:
        families.append("consecutive_limit")
    if settings.enforce_early_late_equal_hours:
        families.append("equal_hours")
    if settings.enforce_late_longer_than_early:
        families.append("late_longer_than_early")
    if settings.enforce_shift_work_caps:
        families.append("shift_work_caps")
    tested = 0
    minimal_relax_sets: list[tuple[str, ...]] = []
    any_relaxation_feasible = False

    for k in range(1, len(families) + 1):
        found_this_k: list[tuple[str, ...]] = []
        for combo in combinations(families, k):
            flags = _default_hard_flags(settings)
            for key in combo:
                flags = HardConstraintFlags(
                    auto_gate_limit=False if key == "auto_gate_limit" else flags.auto_gate_limit,
                    consecutive_limit=False if key == "consecutive_limit" else flags.consecutive_limit,
                    equal_hours=False if key == "equal_hours" else flags.equal_hours,
                    late_longer_than_early=False if key == "late_longer_than_early" else flags.late_longer_than_early,
                    shift_work_caps=False if key == "shift_work_caps" else flags.shift_work_caps,
                )
            res = solve(
                data,
                settings,
                mode_override="hard",
                optimize_soft=False,
                hard_flags=flags,
                time_sec_override=time_limit_per_case_sec,
                locked_role_assignments=locked_role_assignments,
                locked_work_state=locked_work_state,
            )
            tested += 1
            if res.feasible:
                any_relaxation_feasible = True
                found_this_k.append(combo)
        if found_this_k:
            minimal_relax_sets = found_this_k
            break

    return UnsatDiagnosis(
        minimal_relax_sets=minimal_relax_sets,
        tested_cases=tested,
        any_relaxation_feasible=any_relaxation_feasible,
    )


def post_check(data: ProblemData, result: SolveResult, settings: SolverSettings) -> pd.DataFrame:
    rows = []
    num_t = len(data.time_labels)
    num_e = len(data.employees)

    # Coverage + role unique + skill + on-duty window checks
    for t in range(num_t):
        for r in data.roles:
            assigned_people = [e for e in range(num_e) if result.assign.get((e, t)) == r]
            req = data.demand[(t, r)]
            if len(assigned_people) < req:
                rows.append({"type": "coverage", "time": data.time_labels[t], "entity": r, "detail": f"assigned={len(assigned_people)} < demand={req}"})

    # per employee/time unique + skill + on duty
    for e_idx, emp in enumerate(data.employees):
        for t in range(num_t):
            role = result.assign.get((e_idx, t), None)
            if role is None:
                continue
            if not result.on_duty[e_idx][t]:
                rows.append({"type": "on_duty_window", "time": data.time_labels[t], "entity": emp.name, "detail": f"assigned out of shift window role={role}"})
            if role not in emp.skills:
                rows.append({"type": "skill", "time": data.time_labels[t], "entity": emp.name, "detail": f"no skill for role={role}"})

    # auto-gate consecutive + max consecutive
    for e_idx, emp in enumerate(data.employees):
        for t in range(num_t - 1):
            if _is_auto_gate_role(result.assign.get((e_idx, t), "")) and _is_auto_gate_role(result.assign.get((e_idx, t + 1), "")):
                tl = data.time_labels[t]
                rows.append({"type": "auto_gate_consecutive", "time": tl, "entity": emp.name, "detail": "連續兩格自動通關"})

        work_bits = [int(result.work.get((e_idx, t), 0)) for t in range(num_t)]
        total_slots = sum(work_bits)
        longest = longest_consecutive_ones(work_bits)
        try:
            start_h = int(str(emp.shift_start).split(":")[0])
        except Exception:
            start_h = -1
        hard_max_consec = settings.early_max_consecutive_work_slots if start_h in (5, 6) else settings.max_consecutive_work_slots
        if longest > hard_max_consec:
            rows.append({"type": "max_consecutive", "time": "ALL", "entity": emp.name, "detail": f"longest={longest} > {hard_max_consec}"})
        if settings.enforce_shift_work_caps:
            if start_h in (5, 6) and total_slots > settings.early_max_work_slots:
                rows.append({"type": "early_work_limit", "time": "ALL", "entity": emp.name, "detail": f"worked_slots={total_slots} > {settings.early_max_work_slots}"})
            if start_h in (7, 8) and total_slots > settings.late_max_work_slots:
                rows.append({"type": "late_work_limit", "time": "ALL", "entity": emp.name, "detail": f"worked_slots={total_slots} > {settings.late_max_work_slots}"})

    early_vals = []
    late_vals = []
    for e_idx, emp in enumerate(data.employees):
        total_slots = sum(int(result.work.get((e_idx, t), 0)) for t in range(num_t))
        try:
            start_h = int(str(emp.shift_start).split(":")[0])
        except Exception:
            start_h = -1
        if start_h in (5, 6):
            early_vals.append((emp.name, total_slots))
        elif start_h in (7, 8):
            late_vals.append((emp.name, total_slots))

    if settings.enforce_early_late_equal_hours:
        for gname, vals in [("early_equal_hours", early_vals), ("late_equal_hours", late_vals)]:
            if len(vals) <= 1:
                continue
            uniq = sorted({v for _, v in vals})
            if len(uniq) > 1:
                detail = ", ".join([f"{n}:{v}" for n, v in vals])
                rows.append({"type": gname, "time": "ALL", "entity": "group", "detail": detail})

    if settings.enforce_late_longer_than_early and early_vals and late_vals:
        min_gap = max(1, int(settings.min_late_minus_early_slots))
        early_total = sum(v for _, v in early_vals)
        late_total = sum(v for _, v in late_vals)
        lhs = len(early_vals) * late_total
        rhs = len(late_vals) * early_total + min_gap * len(early_vals) * len(late_vals)
        if lhs < rhs:
            early_avg = early_total / max(1, len(early_vals))
            late_avg = late_total / max(1, len(late_vals))
            rows.append(
                {
                    "type": "late_longer_than_early",
                    "time": "ALL",
                    "entity": "group",
                    "detail": f"late_avg={late_avg:.2f}, early_avg={early_avg:.2f}, required_gap={min_gap}",
                }
            )

    if not rows:
        return pd.DataFrame(columns=["type", "time", "entity", "detail"])
    return pd.DataFrame(rows)


def infeasibility_summary(
    data: ProblemData,
    result: SolveResult,
    settings: SolverSettings,
    *,
    audit: FeasibilityAudit | None = None,
    diagnosis: UnsatDiagnosis | None = None,
) -> str:
    # Works best with shortage-mode solution; if none, provide static diagnosis.
    lines = ["Infeasibility Summary", "===================="]
    num_t = len(data.time_labels)

    if result.shortage:
        rows = []
        for (t, r), sh in result.shortage.items():
            if sh > 0:
                rows.append((sh, t, r))
        rows.sort(reverse=True)
        if rows:
            lines.append("Top shortages (time, role, shortage):")
            for sh, t, r in rows[:10]:
                on_duty = 0
                skilled = 0
                for e_idx, emp in enumerate(data.employees):
                    if result.on_duty[e_idx][t]:
                        on_duty += 1
                        if r in emp.skills:
                            skilled += 1
                reason = []
                if skilled < data.demand[(t, r)]:
                    reason.append(f"技能不足({skilled}<{data.demand[(t,r)]})")
                total_req = sum(data.demand[(t, rr)] for rr in data.roles)
                if on_duty < total_req:
                    reason.append(f"人力不足({on_duty}<{total_req})")
                if not reason:
                    reason.append("可能受連續上班上限與同時唯一限制影響")
                lines.append(f"- {data.time_labels[t]} | {r} | shortage={sh} | {'；'.join(reason)}")

    if audit is not None:
        lines.append("")
        lines.append("Feasibility Auditor:")
        lines.append(f"- feasible={audit.feasible}")
        if not audit.feasible and not audit.issues_df.empty:
            for _, row in audit.issues_df.head(12).iterrows():
                lines.append(f"- {row.get('check','')} | {row.get('time','')} | {row.get('role','')} | {row.get('detail','')}")

    if diagnosis is not None:
        lines.append("")
        lines.append("Unsat Diagnosis:")
        lines.append(f"- tested_cases={diagnosis.tested_cases}")
        lines.append(f"- any_relaxation_feasible={diagnosis.any_relaxation_feasible}")
        if diagnosis.minimal_relax_sets:
            lines.append("- minimal_relax_sets:")
            for rs in diagnosis.minimal_relax_sets:
                lines.append(f"  * {', '.join(rs)}")
        else:
            lines.append("- minimal_relax_sets: (none found within diagnosis budget)")

    lines.append("")
    lines.append("Global possible causes:")
    lines.append("- 可用人力總量低於需求總量")
    lines.append("- 特定職位技能覆蓋不足")
    lines.append("- 連續上班上限過緊（早班2.5h、晚班3h）")
    if settings.enforce_early_late_equal_hours:
        lines.append("- 早班/晚班工時完全平均（硬限制）可能過緊")
    if settings.enforce_late_longer_than_early:
        lines.append("- 晚班工時必須高於早班（硬限制）可能過緊")
    if settings.feasibility_mode == "allow_shortage" and settings.weight_last_hour_work >= settings.weight_shortage_slot:
        lines.append("- 下班前1小時懲罰可能過高，導致傾向保留缺口")
    return "\n".join(lines)


def _build_dispatch_table(data: ProblemData, result: SolveResult) -> pd.DataFrame:
    preferred_role_order = ["公務台", "公協", "查驗台1", "查驗台4", "查驗台3", "自動通關", "發證"]
    role_order = [r for r in preferred_role_order if r in data.roles] + [r for r in data.roles if r not in preferred_role_order]
    rows: list[dict[str, Any]] = []
    num_t = len(data.time_labels)

    for t0 in range(0, num_t, 2):
        time_label = data.time_labels[t0]
        hour = int(time_label.split(":")[0])
        row: dict[str, Any] = {"時間": f"{hour}-{hour + 1}"}
        half_hours = [t0]
        if t0 + 1 < num_t:
            half_hours.append(t0 + 1)

        for role in role_order:
            names: list[str] = []
            for t in half_hours:
                assigned = [
                    data.employees[e_idx].name
                    for e_idx in range(len(data.employees))
                    if result.assign.get((e_idx, t), None) == role
                ]
                for name in assigned:
                    if name not in names:
                        names.append(name)
            row[_display_role(role)] = "\n".join(names)
        rows.append(row)

    return pd.DataFrame(rows)


def build_output_tables(data: ProblemData, result: SolveResult, settings: SolverSettings) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    num_t = len(data.time_labels)
    dispatch_df = _build_dispatch_table(data, result)

    assign_rows = []
    for e_idx, emp in enumerate(data.employees):
        row = {"name": emp.name, "shift_window": f"{emp.shift_start}-{emp.shift_end}"}
        end_min = _time_to_min(emp.shift_end)
        for t, tl in enumerate(data.time_labels):
            role = result.assign.get((e_idx, t), None)
            if role is not None:
                row[tl] = role
            else:
                row[tl] = "BREAK" if result.on_duty[e_idx][t] else "OFF"
        row["target_minutes"] = emp.target_slots * 30
        worked = sum(int(result.work.get((e_idx, t), 0)) for t in range(num_t))
        row["worked_minutes"] = worked * 30
        row["last_hour_work_minutes"] = sum(
            30
            for t, tl in enumerate(data.time_labels)
            if result.work.get((e_idx, t), 0) == 1 and (_time_to_min(tl) >= end_min - 60 and _time_to_min(tl) < end_min)
        )
        assign_rows.append(row)
    assignment_df = pd.DataFrame(assign_rows)

    cov_rows = []
    for t, tl in enumerate(data.time_labels):
        for r in data.roles:
            assigned = sum(1 for e in range(len(data.employees)) if result.assign.get((e, t), None) == r)
            req = data.demand[(t, r)]
            sh = result.shortage.get((t, r), max(0, req - assigned))
            cov_rows.append({
                "time": tl,
                "role": r,
                "demand": req,
                "assigned": assigned,
                "shortage": sh,
                "covered": sh == 0,
            })
    coverage_df = pd.DataFrame(cov_rows)

    summary_rows = []
    for e_idx, emp in enumerate(data.employees):
        work_bits = [int(result.work.get((e_idx, t), 0)) for t in range(num_t)]
        total_slots = sum(work_bits)
        auto_slots = sum(1 for t in range(num_t) if _is_auto_gate_role(result.assign.get((e_idx, t), "")))
        longest = longest_consecutive_ones(work_bits)
        has_auto_skill = any(_is_auto_gate_role(sk) for sk in emp.skills)
        try:
            start_h = int(str(emp.shift_start).split(":")[0])
        except Exception:
            start_h = -1
        allowed_slots = settings.early_max_work_slots if start_h in (5, 6) else (
            settings.late_max_work_slots if start_h in (7, 8) else None
        )
        over_cap_slots = max(0, total_slots - allowed_slots) if allowed_slots is not None else 0
        summary_rows.append({
            "name": emp.name,
            "shift_window": f"{emp.shift_start}-{emp.shift_end}",
            "shift_group": "Early" if start_h in (5, 6) else ("Late" if start_h in (7, 8) else "Other"),
            "dedicated_roles": ",".join(sorted(emp.dedicated_roles)),
            "has_auto_gate_skill": int(has_auto_skill),
            "worked_slots": total_slots,
            "worked_minutes": total_slots * 30,
            "shift_group_cap_slots": allowed_slots if allowed_slots is not None else "",
            "shift_group_cap_minutes": (allowed_slots * 30) if allowed_slots is not None else "",
            "over_shift_cap_slots": over_cap_slots,
            "over_shift_cap_minutes": over_cap_slots * 30,
            "target_slots": emp.target_slots,
            "target_minutes": emp.target_slots * 30,
            "target_gap_slots": total_slots - emp.target_slots,
            "auto_gate_slots": auto_slots,
            "auto_gate_minutes": auto_slots * 30,
            "longest_consecutive_slots": longest,
            "longest_consecutive_minutes": longest * 30,
        })
    summary_df = pd.DataFrame(summary_rows)

    return dispatch_df, assignment_df, coverage_df, summary_df


def write_outputs(
    output_excel_path: str,
    report_path: str,
    data: ProblemData,
    result: SolveResult,
    settings: SolverSettings,
    dry_stats: DryRunStats | None = None,
    audit: FeasibilityAudit | None = None,
    diagnosis: UnsatDiagnosis | None = None,
    phase_notes: list[str] | None = None,
) -> None:
    dispatch_df, assignment_df, coverage_df, summary_df = build_output_tables(data, result, settings)
    violations_df = post_check(data, result, settings)

    out = Path(output_excel_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        dispatch_df.to_excel(writer, sheet_name="Dispatch", index=False)
        assignment_df.to_excel(writer, sheet_name="Assignment", index=False)
        coverage_df.to_excel(writer, sheet_name="Coverage", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        violations_df.to_excel(writer, sheet_name="Violations", index=False)
        if audit is not None:
            audit.issues_df.to_excel(writer, sheet_name="AuditIssues", index=False)
        if diagnosis is not None:
            diag_rows = []
            if diagnosis.minimal_relax_sets:
                for i, combo in enumerate(diagnosis.minimal_relax_sets, start=1):
                    diag_rows.append({"rank": i, "relax_set": ",".join(combo)})
            diag_df = pd.DataFrame(diag_rows if diag_rows else [{"rank": "", "relax_set": ""}])
            diag_df["tested_cases"] = diagnosis.tested_cases
            diag_df["any_relaxation_feasible"] = int(diagnosis.any_relaxation_feasible)
            diag_df.to_excel(writer, sheet_name="UnsatDiagnosis", index=False)

    lines = []
    lines.append(f"status={result.status}")
    lines.append(f"mode_used={result.mode_used}")
    lines.append(f"all_covered={int(coverage_df['shortage'].sum()) == 0}")
    lines.append(f"total_shortage_slots={int(coverage_df['shortage'].sum())}")
    if phase_notes:
        lines.append("")
        lines.append("Phase Notes")
        lines.append("-----------")
        for note in phase_notes:
            lines.append(f"- {note}")

    if dry_stats is not None:
        lines.append("")
        lines.append("Dry-run Stats")
        lines.append("------------")
        lines.append(f"total_demand_slots={dry_stats.total_demand_slots}")
        lines.append(f"total_on_duty_slots={dry_stats.total_on_duty_slots}")
        lines.append(f"skill_gap_rows={len(dry_stats.role_skill_gap_rows)}")

    lines.append("")
    lines.append("Violations Summary")
    lines.append("------------------")
    if violations_df.empty:
        lines.append("(no violations)")
    else:
        for _, row in violations_df.iterrows():
            lines.append(f"- {row['type']} | {row['time']} | {row['entity']} | {row['detail']}")

    if int(coverage_df["shortage"].sum()) > 0:
        lines.append("")
        lines.append(infeasibility_summary(data, result, settings, audit=audit, diagnosis=diagnosis))
    elif audit is not None and not audit.feasible:
        lines.append("")
        lines.append(infeasibility_summary(data, result, settings, audit=audit, diagnosis=diagnosis))

    rp = Path(report_path)
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text("\n".join(lines), encoding="utf-8")


def run_pipeline(
    input_path: str,
    output_excel_path: str,
    report_path: str,
    settings: SolverSettings,
    dry_run: bool = False,
    fallback_to_allow_shortage: bool = False,
    locked_role_assignments: dict[tuple[int, int], str] | None = None,
    locked_work_state: dict[tuple[int, int], int] | None = None,
) -> dict[str, Any]:
    emp_df, dem_df = read_input(input_path)
    validate_input(emp_df, dem_df)
    data = build_problem_data(emp_df, dem_df)
    dry_stats = dry_run_stats(data)
    audit = feasibility_audit(data, settings)
    phase_notes: list[str] = []

    if dry_run:
        # produce an empty schedule output with diagnostics-only pass
        empty_result = SolveResult(
            status="DRY_RUN",
            feasible=True,
            mode_used=settings.feasibility_mode,
            assign={},
            work={},
            shortage={(t, r): data.demand[(t, r)] for t in range(len(data.time_labels)) for r in data.roles},
            on_duty=_build_on_duty_map(data),
            objective=None,
        )
        write_outputs(
            output_excel_path,
            report_path,
            data,
            empty_result,
            settings,
            dry_stats=dry_stats,
            audit=audit,
            diagnosis=None,
            phase_notes=["dry-run only: skipped solver"],
        )
        return {
            "status": "DRY_RUN",
            "all_covered": False,
            "total_shortage_slots": dry_stats.total_demand_slots,
            "output_excel": str(Path(output_excel_path).resolve()),
            "report": str(Path(report_path).resolve()),
            "dry_total_demand_slots": dry_stats.total_demand_slots,
            "dry_total_on_duty_slots": dry_stats.total_on_duty_slots,
            "dry_skill_gap_rows": int(len(dry_stats.role_skill_gap_rows)),
            "audit_feasible": bool(audit.feasible),
            "audit_issue_rows": int(len(audit.issues_df)),
        }

    diagnosis: UnsatDiagnosis | None = None

    if settings.feasibility_mode == "hard" and not audit.feasible:
        phase_notes.append("feasibility_audit failed; skipped hard solve")
        diagnosis = diagnose_unsat_constraints(
            data,
            settings,
            time_limit_per_case_sec=max(2, min(6, settings.max_time_sec // 4)),
            locked_role_assignments=locked_role_assignments,
            locked_work_state=locked_work_state,
        )
        diag = solve(
            data,
            settings,
            mode_override="allow_shortage",
            optimize_soft=True,
            time_sec_override=max(2, min(settings.max_time_sec, 12)),
            locked_role_assignments=locked_role_assignments,
            locked_work_state=locked_work_state,
        )
        if diag.feasible:
            diag.status = f"PRECHECK_INFEASIBLE; RELAXED_{diag.status}"
            result = diag
            cov_short = sum(result.shortage.values()) if result.shortage else 0
        else:
            result = SolveResult(
                status="PRECHECK_INFEASIBLE",
                feasible=False,
                mode_used="hard",
                assign={},
                work={},
                shortage={(t, r): data.demand[(t, r)] for t in range(len(data.time_labels)) for r in data.roles},
                on_duty=_build_on_duty_map(data),
                objective=None,
            )
            cov_short = sum(result.shortage.values()) if result.shortage else 0
        write_outputs(
            output_excel_path,
            report_path,
            data,
            result,
            settings,
            dry_stats=dry_stats,
            audit=audit,
            diagnosis=diagnosis,
            phase_notes=phase_notes,
        )
        return {
            "status": result.status,
            "mode_used": "hard",
            "all_covered": False,
            "total_shortage_slots": int(cov_short),
            "output_excel": str(Path(output_excel_path).resolve()),
            "report": str(Path(report_path).resolve()),
            "dry_total_demand_slots": dry_stats.total_demand_slots,
            "dry_total_on_duty_slots": dry_stats.total_on_duty_slots,
            "dry_skill_gap_rows": int(len(dry_stats.role_skill_gap_rows)),
            "hard_infeasible": True,
            "audit_feasible": bool(audit.feasible),
            "audit_issue_rows": int(len(audit.issues_df)),
        }

    # Two-phase solving:
    # Phase A: hard-feasible search only.
    phase_notes.append("phase_a_start: hard feasibility")
    phase_a = solve(
        data,
        settings,
        mode_override=settings.feasibility_mode,
        optimize_soft=False,
        locked_role_assignments=locked_role_assignments,
        locked_work_state=locked_work_state,
    )
    if not phase_a.feasible:
        phase_notes.append(f"phase_a_infeasible: {phase_a.status}")
        if settings.feasibility_mode == "hard":
            if fallback_to_allow_shortage:
                result = solve(
                    data,
                    settings,
                    mode_override="allow_shortage",
                    optimize_soft=True,
                    locked_role_assignments=locked_role_assignments,
                    locked_work_state=locked_work_state,
                )
                phase_notes.append("fallback_to_allow_shortage=true")
            else:
                diagnosis = diagnose_unsat_constraints(
                    data,
                    settings,
                    time_limit_per_case_sec=max(2, min(6, settings.max_time_sec // 4)),
                    locked_role_assignments=locked_role_assignments,
                    locked_work_state=locked_work_state,
                )
                result = solve(
                    data,
                    settings,
                    mode_override="allow_shortage",
                    optimize_soft=True,
                    time_sec_override=max(2, min(settings.max_time_sec, 12)),
                    locked_role_assignments=locked_role_assignments,
                    locked_work_state=locked_work_state,
                )
                if result.feasible:
                    result.status = f"HARD_{phase_a.status}; RELAXED_{result.status}"
                else:
                    result = SolveResult(
                        status=phase_a.status,
                        feasible=False,
                        mode_used=settings.feasibility_mode,
                        assign={},
                        work={},
                        shortage={(t, r): data.demand[(t, r)] for t in range(len(data.time_labels)) for r in data.roles},
                        on_duty=_build_on_duty_map(data),
                        objective=None,
                    )
        else:
            result = phase_a
    else:
        phase_notes.append("phase_a_feasible")
        # Phase B: optimize with soft objectives, seeded by phase-A hint.
        phase_b = solve(
            data,
            settings,
            mode_override=settings.feasibility_mode,
            optimize_soft=True,
            hint_result=phase_a,
            locked_role_assignments=locked_role_assignments,
            locked_work_state=locked_work_state,
        )
        if phase_b.feasible:
            phase_notes.append("phase_b_feasible")
            result = phase_b
        else:
            phase_notes.append(f"phase_b_infeasible_fallback_to_phase_a: {phase_b.status}")
            result = phase_a

    write_outputs(
        output_excel_path,
        report_path,
        data,
        result,
        settings,
        dry_stats=dry_stats,
        audit=audit,
        diagnosis=diagnosis,
        phase_notes=phase_notes,
    )
    cov_short = sum(result.shortage.values()) if result.shortage else 0
    return {
        "status": result.status,
        "mode_used": result.mode_used,
        "all_covered": cov_short == 0,
        "total_shortage_slots": int(cov_short),
        "output_excel": str(Path(output_excel_path).resolve()),
        "report": str(Path(report_path).resolve()),
        "dry_total_demand_slots": dry_stats.total_demand_slots,
        "dry_total_on_duty_slots": dry_stats.total_on_duty_slots,
        "dry_skill_gap_rows": int(len(dry_stats.role_skill_gap_rows)),
        "audit_feasible": bool(audit.feasible),
        "audit_issue_rows": int(len(audit.issues_df)),
        "hard_infeasible": bool(settings.feasibility_mode == "hard" and not result.feasible),
    }


def generate_sample_input(path: str) -> str:
    # Minimal runnable sample
    times = []
    cur = _time_to_min("05:00")
    end = _time_to_min("19:30")
    while cur <= end:
        times.append(f"{cur//60:02d}:{cur%60:02d}")
        cur += 30

    employees = pd.DataFrame(
        [
            {
                "name": "A01",
                "shift_start": "05:00",
                "shift_end": "16:00",
                "skills": "公務台,查驗台1,自動通關",
                "target_work_minutes": 390,
            },
            {
                "name": "A02",
                "shift_start": "05:00",
                "shift_end": "16:00",
                "skills": "公務台協勤,查驗台3,自動通關",
                "target_work_minutes": 390,
            },
            {
                "name": "B01",
                "shift_start": "06:00",
                "shift_end": "17:00",
                "skills": "公務台,查驗台4,發證",
                "target_work_minutes": 390,
            },
            {
                "name": "C01",
                "shift_start": "07:00",
                "shift_end": "19:00",
                "skills": "查驗台1,查驗台3,自動通關",
                "target_work_minutes": 420,
            },
            {
                "name": "D01",
                "shift_start": "08:00",
                "shift_end": "20:00",
                "skills": "查驗台4,發證,自動通關",
                "target_work_minutes": 420,
            },
        ]
    )

    demand_rows = []
    for tl in times:
        demand_rows.append(
            {
                "time": tl,
                "公務台": 1 if tl < "17:00" else 0,
                "查驗台1": 1 if "06:00" <= tl < "18:00" else 0,
                "自動通關": 1 if "07:00" <= tl < "19:00" else 0,
                "發證": 1 if "08:00" <= tl < "16:00" else 0,
            }
        )
    demand = pd.DataFrame(demand_rows)

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(p, engine="openpyxl") as writer:
        employees.to_excel(writer, sheet_name="Employees", index=False)
        demand.to_excel(writer, sheet_name="Demand", index=False)
    return str(p.resolve())
