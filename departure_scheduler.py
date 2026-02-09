from __future__ import annotations

from 機場排班程式 import run_scheduler, validate_input_excel


def validate_departure_input_excel(input_excel_path: str) -> list[dict]:
    """Validation entrypoint for departure-duty scheduling."""
    return validate_input_excel(input_excel_path)


def run_departure_scheduler(
    input_excel_path: str,
    output_excel_path: str | None = None,
    *,
    days_limit: int | None = None,
    include_external: bool = False,
    search_best_roster: bool = True,
    search_patience: int = 5,
    require_all_pulls_nonzero: bool = False,
    reset_before_sched: bool = True,
    smart_team_pick: bool = True,
    debug: bool = False,
    random_seed: int | None = None,
    progress_callback=None,
    priority_mode: str = "team1",
    custom_order: str = "fairness,shift_count",
    rescue_fill: bool = True,
    score_order: str = "fairness,shift,pull",
) -> dict:
    """Departure-duty scheduling entrypoint.

    Currently reuses monthly scheduler; keep this file for departure-specific rules.
    """
    return run_scheduler(
        input_excel_path=input_excel_path,
        output_excel_path=output_excel_path,
        days_limit=days_limit,
        include_external=include_external,
        search_best_roster=search_best_roster,
        search_patience=search_patience,
        require_all_pulls_nonzero=require_all_pulls_nonzero,
        reset_before_sched=reset_before_sched,
        smart_team_pick=smart_team_pick,
        debug=debug,
        random_seed=random_seed,
        progress_callback=progress_callback,
        priority_mode=priority_mode,
        custom_order=custom_order,
        rescue_fill=rescue_fill,
        score_order=score_order,
    )
