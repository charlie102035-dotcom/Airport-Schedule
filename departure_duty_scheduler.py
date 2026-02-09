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
    - or bool columns prefixed by `skill_`, e.g. skill_公務台, skill_查驗台1
  Optional:
    - target_work_minutes

- Sheet `Demand`
  Required columns:
    - time: 30-min slots from 05:00 ... 19:30
    - role columns: each column is a role name, value is required headcount (>=0)
"""
from __future__ import annotations

from dataclasses import dataclass
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
    target_slots: int


@dataclass(frozen=True)
class SolverSettings:
    weight_last_hour_work: int = 50
    weight_group_fairness: int = 8
    weight_target_deviation: int = 3
    weight_same_hour_consistency: int = 12
    weight_single_slot_fragment: int = 18
    weight_shortage_slot: int = 100000
    auto_gate_max_slots: int = 6
    max_consecutive_work_slots: int = 6
    early_max_work_slots: int = 14  # 7.0 hours
    late_max_work_slots: int = 15   # 7.5 hours
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


def _is_auto_gate_role(role_name: str) -> bool:
    s = _canon_role(str(role_name).strip())
    return ("自動通關" in s) or (s == "E-Gate")


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
        target_slots = _calc_target_slots(s, e, row.get(target_col, None) if target_col is not None else None)
        employees.append(Employee(name=name, shift_start=s, shift_end=e, skills=skills, target_slots=target_slots))

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


def _build_on_duty_map(data: ProblemData) -> list[list[bool]]:
    on_duty = [[False] * len(data.time_labels) for _ in data.employees]
    for e_idx, emp in enumerate(data.employees):
        s_min = _time_to_min(emp.shift_start)
        e_min = _time_to_min(emp.shift_end)
        for t, tl in enumerate(data.time_labels):
            tm = _time_to_min(tl)
            on_duty[e_idx][t] = s_min <= tm < e_min
    return on_duty


def solve(data: ProblemData, settings: SolverSettings, mode_override: str | None = None) -> SolveResult:
    mode = mode_override or settings.feasibility_mode
    strict = (mode == "hard")

    model = cp_model.CpModel()
    num_e = len(data.employees)
    num_t = len(data.time_labels)
    on_duty = _build_on_duty_map(data)

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
    for t in range(num_t):
        for r in data.roles:
            seat_used_vars = []
            for s in seats[r]:
                assign_vars = [x[(e, t, r, s)] for e in range(num_e) if (e, t, r, s) in x]
                if assign_vars:
                    used = model.NewBoolVar(f"seat_used_t{t}_r{r}_s{s}")
                    model.Add(sum(assign_vars) == used)
                    seat_used_vars.append(used)
                else:
                    seat_used_vars.append(model.NewConstant(0))

            assigned_cnt = sum(seat_used_vars)
            req = data.demand[(t, r)]
            if strict:
                model.Add(assigned_cnt == req)
            else:
                sh = model.NewIntVar(0, req, f"short_t{t}_r{r}")
                shortage[(t, r)] = sh
                model.Add(assigned_cnt + sh == req)

    # auto-gate limit
    auto_roles = [r for r in data.roles if _is_auto_gate_role(r)]
    if auto_roles:
        for e_idx in range(num_e):
            auto_vars = [
                x[(e_idx, t, r, s)]
                for t in range(num_t)
                for r in auto_roles
                for s in seats[r]
                if (e_idx, t, r, s) in x
            ]
            if auto_vars:
                model.Add(sum(auto_vars) <= settings.auto_gate_max_slots)

    # max consecutive work slots
    max_c = settings.max_consecutive_work_slots
    for e_idx in range(num_e):
        for st in range(0, num_t - max_c):
            model.Add(sum(work[(e_idx, t)] for t in range(st, st + max_c + 1)) <= max_c)

    # objective terms
    penalties: list[cp_model.LinearExpr] = []

    # A) last hour penalty
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
            if settings.enforce_shift_work_caps:
                model.Add(var <= group_cap)
            else:
                excess = model.NewIntVar(0, cap, f"cap_excess_e{e_idx}")
                model.Add(excess >= var - group_cap)
                if settings.weight_shift_cap_excess > 0:
                    penalties.append(settings.weight_shift_cap_excess * excess)
        total_slots_vars.append(var)

    # B) fairness within same window
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

    # C) target deviation
    for e_idx, emp in enumerate(data.employees):
        dev = model.NewIntVar(0, num_t, f"dev_e{e_idx}")
        model.AddAbsEquality(dev, total_slots_vars[e_idx] - emp.target_slots)
        penalties.append(settings.weight_target_deviation * dev)

    if not strict and shortage:
        penalties.append(settings.weight_shortage_slot * sum(shortage.values()))

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

    model.Minimize(sum(penalties) if penalties else 0)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = max(1, settings.max_time_sec)
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

    # max auto-gate + max consecutive
    for e_idx, emp in enumerate(data.employees):
        auto_slots = sum(
            1
            for t in range(num_t)
            if _is_auto_gate_role(result.assign.get((e_idx, t), ""))
        )
        if auto_slots > settings.auto_gate_max_slots:
            rows.append({"type": "auto_gate_limit", "time": "ALL", "entity": emp.name, "detail": f"auto_slots={auto_slots} > {settings.auto_gate_max_slots}"})

        work_bits = [int(result.work.get((e_idx, t), 0)) for t in range(num_t)]
        total_slots = sum(work_bits)
        longest = longest_consecutive_ones(work_bits)
        if longest > settings.max_consecutive_work_slots:
            rows.append({"type": "max_consecutive", "time": "ALL", "entity": emp.name, "detail": f"longest={longest} > {settings.max_consecutive_work_slots}"})
        try:
            start_h = int(str(emp.shift_start).split(":")[0])
        except Exception:
            start_h = -1
        if settings.enforce_shift_work_caps:
            if start_h in (5, 6) and total_slots > settings.early_max_work_slots:
                rows.append({"type": "early_work_limit", "time": "ALL", "entity": emp.name, "detail": f"worked_slots={total_slots} > {settings.early_max_work_slots}"})
            if start_h in (7, 8) and total_slots > settings.late_max_work_slots:
                rows.append({"type": "late_work_limit", "time": "ALL", "entity": emp.name, "detail": f"worked_slots={total_slots} > {settings.late_max_work_slots}"})

    if not rows:
        return pd.DataFrame(columns=["type", "time", "entity", "detail"])
    return pd.DataFrame(rows)


def infeasibility_summary(data: ProblemData, result: SolveResult, settings: SolverSettings) -> str:
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

    lines.append("")
    lines.append("Global possible causes:")
    lines.append("- 可用人力總量低於需求總量")
    lines.append("- 特定職位技能覆蓋不足")
    lines.append("- 連續上班上限過緊（可考慮調整 max_consecutive_work_slots）")
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

    lines = []
    lines.append(f"status={result.status}")
    lines.append(f"mode_used={result.mode_used}")
    lines.append(f"all_covered={int(coverage_df['shortage'].sum()) == 0}")
    lines.append(f"total_shortage_slots={int(coverage_df['shortage'].sum())}")

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
        lines.append(infeasibility_summary(data, result, settings))

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
) -> dict[str, Any]:
    emp_df, dem_df = read_input(input_path)
    validate_input(emp_df, dem_df)
    data = build_problem_data(emp_df, dem_df)
    dry_stats = dry_run_stats(data)

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
        write_outputs(output_excel_path, report_path, data, empty_result, settings, dry_stats=dry_stats)
        return {
            "status": "DRY_RUN",
            "all_covered": False,
            "total_shortage_slots": dry_stats.total_demand_slots,
            "output_excel": str(Path(output_excel_path).resolve()),
            "report": str(Path(report_path).resolve()),
            "dry_total_demand_slots": dry_stats.total_demand_slots,
            "dry_total_on_duty_slots": dry_stats.total_on_duty_slots,
            "dry_skill_gap_rows": int(len(dry_stats.role_skill_gap_rows)),
        }

    # Solve in configured mode. In hard mode, fallback can be enabled/disabled.
    result = solve(data, settings)
    if (not result.feasible) and settings.feasibility_mode == "hard":
        if fallback_to_allow_shortage:
            result = solve(data, settings, mode_override="allow_shortage")
        else:
            # Keep hard rule strict for final schedule. We still run relaxed mode only
            # for diagnostics/reporting to explain where shortages come from.
            diag = solve(data, settings, mode_override="allow_shortage")
            if diag.feasible:
                diag.status = f"HARD_{result.status}; RELAXED_{diag.status}"
                write_outputs(output_excel_path, report_path, data, diag, settings, dry_stats=dry_stats)
                cov_short = sum(diag.shortage.values()) if diag.shortage else 0
            else:
                placeholder = SolveResult(
                    status=result.status,
                    feasible=False,
                    mode_used=settings.feasibility_mode,
                    assign={},
                    work={},
                    shortage={(t, r): data.demand[(t, r)] for t in range(len(data.time_labels)) for r in data.roles},
                    on_duty=_build_on_duty_map(data),
                    objective=None,
                )
                write_outputs(output_excel_path, report_path, data, placeholder, settings, dry_stats=dry_stats)
                cov_short = sum(placeholder.shortage.values())
            return {
                "status": result.status,
                "mode_used": settings.feasibility_mode,
                "all_covered": False,
                "total_shortage_slots": int(cov_short),
                "output_excel": str(Path(output_excel_path).resolve()),
                "report": str(Path(report_path).resolve()),
                "dry_total_demand_slots": dry_stats.total_demand_slots,
                "dry_total_on_duty_slots": dry_stats.total_on_duty_slots,
                "dry_skill_gap_rows": int(len(dry_stats.role_skill_gap_rows)),
                "hard_infeasible": True,
            }

    write_outputs(output_excel_path, report_path, data, result, settings, dry_stats=dry_stats)
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
