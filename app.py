from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from functools import lru_cache
import time
import uuid
import html
from openpyxl import load_workbook

from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# 讓 Python 找得到同層/上層的「機場排班程式.py」
import sys
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(PROJECT_DIR))

@lru_cache(maxsize=1)
def _get_scheduler_funcs():
    from 機場排班程式 import run_scheduler, validate_input_excel  # noqa: E402

    return run_scheduler, validate_input_excel


app = FastAPI(title="Airport Scheduler MVP")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Simple in-memory store for recent results (token -> metadata)
_RESULTS: dict[str, dict] = {}
_RESULT_TTL_SEC = 60 * 10


def _store_result(
    token: str,
    out_path: Path,
    tmpdir: Path,
    tries: int,
    best_score_100: float,
    chart_data: dict,
) -> None:
    _RESULTS[token] = {
        "out_path": out_path,
        "tmpdir": tmpdir,
        "tries": tries,
        "best_score_100": best_score_100,
        "chart_data": chart_data,
        "ts": time.time(),
        "status": "done",
        "progress": 1.0,
    }


def _pop_result(token: str) -> dict | None:
    data = _RESULTS.pop(token, None)
    return data


def _get_result(token: str) -> dict | None:
    _cleanup_expired_results()
    data = _RESULTS.get(token)
    if data:
        data["ts"] = time.time()
    return data


def _esc(v) -> str:
    return html.escape(str(v if v is not None else ""), quote=True)


def _init_progress(token: str, tmpdir: Path, min_tries: int) -> None:
    _RESULTS[token] = {
        "tmpdir": tmpdir,
        "ts": time.time(),
        "start_ts": time.time(),
        "status": "running",
        "progress": 0.0,
        "tries": 0,
        "min_tries": int(min_tries),
    }


def _set_progress(token: str, current_try: int, max_tries: int) -> None:
    data = _RESULTS.get(token)
    if not data:
        return
    data["ts"] = time.time()
    total = max(1, int(max_tries))
    cur = max(0, int(current_try))
    data["tries"] = cur
    data["progress"] = min(1.0, cur / total)


def _set_error(token: str, msg: str) -> None:
    data = _RESULTS.get(token)
    if not data:
        return
    data["status"] = "error"
    data["message"] = msg


def _format_validation_errors(errors: list[dict]) -> str:
    lines = []
    for err in errors:
        sheet = str(err.get("sheet", "") or "")
        cols = err.get("columns", []) or []
        reason = str(err.get("reason", "") or "")
        cells = err.get("cells", []) or []
        col_text = ", ".join([str(c) for c in cols if str(c).strip() != ""])
        if reason and col_text:
            line = f"{sheet}: {reason} -> {col_text}"
        elif reason:
            line = f"{sheet}: {reason}"
        elif col_text:
            line = f"{sheet}: {col_text}"
        else:
            line = f"{sheet}: 欄位不符合模板"
        lines.append(line)
        if cells:
            for cell in cells:
                day = cell.get("day", None)
                person = str(cell.get("person", "") or "").strip()
                value = str(cell.get("value", "") or "").strip()
                if day is None or person == "":
                    continue
                if value != "":
                    lines.append(f"請檢查{int(day)}號的{person}（值={value}）")
                else:
                    lines.append(f"請檢查{int(day)}號的{person}")
    return "\n".join(lines) if lines else "輸入檔案格式不正確。"


def _hex_color(rgb) -> str:
    if not rgb:
        return ""
    try:
        v = rgb[-6:]
        return f"#{v}"
    except Exception:
        return ""


def _cleanup_expired_results() -> None:
    now = time.time()
    expired = [k for k, v in _RESULTS.items() if now - float(v.get("ts", 0)) > _RESULT_TTL_SEC]
    for k in expired:
        v = _RESULTS.pop(k, None)
        if not v:
            continue
        tmpdir = v.get("tmpdir")
        if isinstance(tmpdir, Path):
            shutil.rmtree(tmpdir, ignore_errors=True)


def _find_manual_preview_url() -> str | None:
    static_dir = BASE_DIR / "static"
    preferred = [
        static_dir / "manual.pdf",
        static_dir / "使用說明書.pdf",
    ]
    candidates: list[Path] = []
    for p in preferred:
        if p.exists() and p.is_file():
            candidates.append(p)

    if not candidates:
        candidates = [p for p in static_dir.glob("*.pdf") if p.is_file()]
    if not candidates:
        return None

    # Always use the newest file and append mtime to bypass browser cache.
    chosen = max(candidates, key=lambda p: p.stat().st_mtime)
    ver = int(chosen.stat().st_mtime)
    return f"/static/{chosen.name}?v={ver}"

    


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    manual_preview_url = _find_manual_preview_url()
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "template_url": "https://drive.google.com/drive/folders/1mNXtRv5olbJQAGhnVy30mBoa8m4nTAJT?usp=sharing",
            "manual_url": "https://drive.google.com/file/d/1ypcRSL7oebprND6yXXhVLAe_QJ2uxFD1/view?usp=share_link",
            "manual_preview_url": manual_preview_url,
        },
    )


@app.post("/run")
async def run(
    background_tasks: BackgroundTasks,
    request: Request,
    file: UploadFile = File(...),
    priority_mode: str = Form("team1"),
    custom_order: str = Form("fairness,shift_count"),
    score_order: str = Form("fairness,shift,pull"),
):
    # 1) 基本檢查：副檔名
    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")
    # 2) 每個 request 使用獨立 temp 資料夾（避免互踩）
    tmpdir = Path(tempfile.mkdtemp(prefix="airport_web_"))
    in_path = tmpdir / "input.xlsx"
    out_path = tmpdir / "output.xlsx"

    try:
        _cleanup_expired_results()
        # 3) 存上傳檔到 temp
        try:
            with in_path.open("wb") as f:
                shutil.copyfileobj(file.file, f)
        finally:
            await file.close()

        # 4) 檔案大小限制（例如 10MB）
        if in_path.stat().st_size > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File too large (max 10MB).")

        # 4.1) 輸入資料檢查：立即回報錯誤欄位並返回首頁
        run_scheduler, validate_input_excel = _get_scheduler_funcs()
        validation_errors = validate_input_excel(str(in_path))
        if validation_errors:
            msg = _format_validation_errors(validation_errors)
            shutil.rmtree(tmpdir, ignore_errors=True)
            return templates.TemplateResponse(
                "validation_error.html",
                {
                    "request": request,
                    "message": msg,
                },
                status_code=400,
            )

        token = uuid.uuid4().hex
        _init_progress(token, tmpdir, 100)

        def _run_job() -> None:
            try:
                def _cb(cur: int, mx: int) -> None:
                    _set_progress(token, cur, mx)

                result = run_scheduler(
                    input_excel_path=str(in_path),
                    output_excel_path=str(out_path),
                    search_best_roster=True,
                    search_patience=10,
                    require_all_pulls_nonzero=False,
                    debug=False,
                    progress_callback=_cb,
                    priority_mode=priority_mode,
                    custom_order=custom_order,
                    rescue_fill=True,
                    score_order=score_order,
                )

                tries_used = int(result.get("tries", 0) or 0)
                best_score = float(result.get("best_score_100", 0.0) or 0.0)
                chart_data = result.get("chart_data", {}) or {}
                _store_result(token, out_path, tmpdir, tries_used, best_score, chart_data)
            except Exception as e:
                _set_error(token, str(e))

        background_tasks.add_task(_run_job)

        return templates.TemplateResponse(
            "running.html",
            {
                "request": request,
                "token": token,
            },
        )

    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


@app.get("/download/{token}")
def download(token: str, background_tasks: BackgroundTasks):
    data = _get_result(token)
    if not data:
        raise HTTPException(status_code=404, detail="Result expired or not found.")

    out_path = data.get("out_path")
    tmpdir = data.get("tmpdir")
    if not isinstance(out_path, Path) or not out_path.exists():
        if isinstance(tmpdir, Path):
            shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=404, detail="File missing.")

    return FileResponse(
        path=str(out_path),
        filename="roster_output.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/progress/{token}")
def progress(token: str):
    data = _get_result(token)
    if not data:
        return JSONResponse({"status": "error", "message": "Result expired or not found."}, status_code=404)
    now = time.time()
    start_ts = float(data.get("start_ts", now) or now)
    tries = int(data.get("tries", 0) or 0)
    max_tries = int(data.get("min_tries", 100) or 100)
    eta_sec = None
    elapsed = max(0.0, now - start_ts)
    if tries > 0 and elapsed > 0.5:
        rate = tries / elapsed
        if rate > 0:
            remaining = max(0, max_tries - tries)
            eta_sec = remaining / rate
    return JSONResponse(
        {
            "status": data.get("status", "running"),
            "progress": float(data.get("progress", 0.0) or 0.0),
            "tries": tries,
            "max_tries": max_tries,
            "eta_sec": eta_sec,
            "message": data.get("message", ""),
        }
    )


@app.get("/preview/{token}", response_class=HTMLResponse)
def preview(token: str, request: Request):
    data = _get_result(token)
    if not data:
        raise HTTPException(status_code=404, detail="Result expired or not found.")

    out_path = data.get("out_path")
    if not isinstance(out_path, Path) or not out_path.exists():
        raise HTTPException(status_code=404, detail="File missing.")

    try:
        wb = load_workbook(out_path, data_only=True)
        ws = wb.active
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel: {e}")

    chart_data = data.get("chart_data", {}) or {}
    shift_charts = chart_data.get("shift", {}) or {}
    skill_charts = chart_data.get("skill", {}) or {}
    pull_charts = chart_data.get("pull", {}) or {}

    max_rows = ws.max_row
    max_cols = ws.max_column

    col_pct = 100 / max(1, max_cols)
    # Build HTML table with basic fill colors + bold
    rows_html = []
    for r in range(1, max_rows + 1):
        cells = []
        for c in range(1, max_cols + 1):
            cell = ws.cell(row=r, column=c)
            val = _esc("" if cell.value is None else str(cell.value))
            styles = []
            fill = cell.fill
            if fill and getattr(fill, "fill_type", None) not in (None, "none"):
                fg = getattr(fill, "fgColor", None)
                rgb = getattr(fg, "rgb", None) if fg else None
                color = _hex_color(rgb)
                if color:
                    styles.append(f"background-color: {color};")
            if cell.font and cell.font.bold:
                styles.append("font-weight: 700;")
            styles.append(f"width:{col_pct:.3f}%")
            styles.append("min-width:110px")
            style_attr = f' style="{" ".join(styles)}"'
            tag = "th" if r == 1 else "td"
            cells.append(f"<{tag}{style_attr}>{val}</{tag}>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    table_html = (
        "<table style='table-layout: fixed; width: 100%; border-collapse: collapse;' "
        "border='1' cellspacing='0' cellpadding='4'>"
        + "".join(rows_html)
        + "</table>"
    )

    # Build charts HTML
    charts_html = ""
    for sh, groups in shift_charts.items():
        a_list = groups.get("A", []) or []
        b_list = groups.get("B", []) or []
        max_val = 0
        for _, v in a_list + b_list:
            try:
                max_val = max(max_val, int(v))
            except Exception:
                pass
        max_val = max(1, max_val)

        def _bars(items, color):
            rows = []
            for name, v in items:
                try:
                    val = int(v)
                except Exception:
                    val = 0
                width = int((val / max_val) * 200)
                safe_name = _esc(name)
                rows.append(
                    f"<div style='display:flex;align-items:center;gap:6px;margin:2px 0;'>"
                    f"<div style='width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{safe_name}</div>"
                    f"<div style='height:12px;width:{width}px;background:{color};'></div>"
                    f"<div style='width:24px;text-align:right;'>{val}</div>"
                    f"</div>"
                )
            return "".join(rows) if rows else "<div style='color:#888;'>無資料</div>"

        charts_html += (
            f"<div style='margin:12px 0;padding:10px;border:1px solid #ddd;'>"
            f"<div style='font-weight:600;margin-bottom:8px;'>{_esc(sh)}</div>"
            f"<div style='display:flex;gap:16px;'>"
            f"<div style='flex:1;border-right:1px solid #eee;padding-right:8px;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>A組</div>"
            f"{_bars(a_list, '#F9E27D')}"
            f"</div>"
            f"<div style='flex:1;padding-left:8px;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>B組</div>"
            f"{_bars(b_list, '#9AD59A')}"
            f"</div>"
            f"</div>"
            f"</div>"
        )

    skill_html = ""
    for sk, groups in skill_charts.items():
        a_list = groups.get("A", []) or []
        b_list = groups.get("B", []) or []
        max_val = 0
        for _, v in a_list + b_list:
            try:
                max_val = max(max_val, int(v))
            except Exception:
                pass
        max_val = max(1, max_val)

        def _bars(items, color):
            rows = []
            for name, v in items:
                try:
                    val = int(v)
                except Exception:
                    val = 0
                width = int((val / max_val) * 200)
                safe_name = _esc(name)
                rows.append(
                    f"<div style='display:flex;align-items:center;gap:6px;margin:2px 0;'>"
                    f"<div style='width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{safe_name}</div>"
                    f"<div style='height:12px;width:{width}px;background:{color};'></div>"
                    f"<div style='width:24px;text-align:right;'>{val}</div>"
                    f"</div>"
                )
            return "".join(rows) if rows else "<div style='color:#888;'>無資料</div>"

        skill_html += (
            f"<div style='margin:12px 0;padding:10px;border:1px solid #ddd;'>"
            f"<div style='font-weight:600;margin-bottom:8px;'>{_esc(sk)}</div>"
            f"<div style='display:flex;gap:16px;'>"
            f"<div style='flex:1;border-right:1px solid #eee;padding-right:8px;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>A組</div>"
            f"{_bars(a_list, '#F9E27D')}"
            f"</div>"
            f"<div style='flex:1;padding-left:8px;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>B組</div>"
            f"{_bars(b_list, '#9AD59A')}"
            f"</div>"
            f"</div>"
            f"</div>"
        )

    pull_html = ""
    for title, groups in pull_charts.items():
        a_list = groups.get("A", []) or []
        b_list = groups.get("B", []) or []
        max_val = 0
        for _, v in a_list + b_list:
            try:
                max_val = max(max_val, int(v))
            except Exception:
                pass
        max_val = max(1, max_val)

        def _bars(items, color):
            rows = []
            for name, v in items:
                try:
                    val = int(v)
                except Exception:
                    val = 0
                width = int((val / max_val) * 200)
                safe_name = _esc(name)
                rows.append(
                    f"<div style='display:flex;align-items:center;gap:6px;margin:2px 0;'>"
                    f"<div style='width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{safe_name}</div>"
                    f"<div style='height:12px;width:{width}px;background:{color};'></div>"
                    f"<div style='width:24px;text-align:right;'>{val}</div>"
                    f"</div>"
                )
            return "".join(rows) if rows else "<div style='color:#888;'>無資料</div>"

        pull_html += (
            f"<div style='margin:12px 0;padding:10px;border:1px solid #ddd;'>"
            f"<div style='font-weight:600;margin-bottom:8px;'>{_esc(title)}</div>"
            f"<div style='display:flex;gap:16px;'>"
            f"<div style='flex:1;border-right:1px solid #eee;padding-right:8px;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>A組</div>"
            f"{_bars(a_list, '#F9E27D')}"
            f"</div>"
            f"<div style='flex:1;padding-left:8px;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>B組</div>"
            f"{_bars(b_list, '#9AD59A')}"
            f"</div>"
            f"</div>"
            f"</div>"
        )

    return templates.TemplateResponse(
        "preview.html",
        {
            "request": request,
            "token": token,
            "table_html": table_html,
            "charts_html": charts_html,
            "skill_html": skill_html,
            "pull_html": pull_html,
        },
    )


@app.get("/report/{token}")
def report_pdf(token: str):
    data = _get_result(token)
    if not data:
        raise HTTPException(status_code=404, detail="Result expired or not found.")

    chart_data = data.get("chart_data", {}) or {}
    shift_charts = chart_data.get("shift", {}) or {}
    skill_charts = chart_data.get("skill", {}) or {}
    pull_charts = chart_data.get("pull", {}) or {}

    # Register Chinese font
    font_path = BASE_DIR / "fonts" / "static" / "NotoSansTC-Regular.ttf"
    if font_path.exists():
        pdfmetrics.registerFont(TTFont("NotoSansTC", str(font_path)))
        base_font = "NotoSansTC"
    else:
        base_font = "Helvetica"

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    def _draw_chart(title: str, groups: dict, y: float) -> float:
        c.setFont(base_font, 10)
        c.drawString(20 * mm, y, title)
        y -= 6 * mm

        a_list = groups.get("A", []) or []
        b_list = groups.get("B", []) or []
        max_val = 1
        for _, v in a_list + b_list:
            try:
                max_val = max(max_val, int(v))
            except Exception:
                pass

        def _draw_group(label: str, items: list, color: str, x0: float, y0: float) -> float:
            c.setFont(base_font, 8)
            c.drawString(x0, y0, label)
            y1 = y0 - 4 * mm
            for name, v in items:
                try:
                    val = int(v)
                except Exception:
                    val = 0
                bar_w = 60 * mm * (val / max_val)
                c.setFillColor(HexColor(color))
                c.rect(x0 + 22 * mm, y1 - 2, bar_w, 3 * mm, stroke=0, fill=1)
                c.setFillColor(HexColor("#000000"))
                c.drawString(x0, y1, str(name)[:10])
                c.drawRightString(x0 + 20 * mm, y1, f"{val}")
                y1 -= 4 * mm
                if y1 < 20 * mm:
                    c.showPage()
                    y1 = height - 20 * mm
            return y1

        left_x = 20 * mm
        right_x = width / 2 + 5 * mm
        y_left = _draw_group("A", a_list, "#F9E27D", left_x, y)
        y_right = _draw_group("B", b_list, "#9AD59A", right_x, y)
        y = min(y_left, y_right) - 8 * mm
        if y < 25 * mm:
            c.showPage()
            y = height - 20 * mm
        return y

    y = height - 20 * mm
    for title, groups in shift_charts.items():
        y = _draw_chart(title, groups, y)
    for title, groups in skill_charts.items():
        y = _draw_chart(title, groups, y)
    for title, groups in pull_charts.items():
        y = _draw_chart(title, groups, y)

    c.save()
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf", headers={"Content-Disposition": "attachment; filename=report.pdf"})


if __name__ == "__main__":
    import os
    import uvicorn

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
