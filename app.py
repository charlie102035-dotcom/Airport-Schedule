from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
import time
import uuid
import pandas as pd
from openpyxl import load_workbook

from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

# 讓 Python 找得到上層的「機場排班程式.py」
import sys
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from 機場排班程式 import run_scheduler  # noqa: E402


app = FastAPI(title="Airport Scheduler MVP")

# Simple in-memory store for recent results (token -> metadata)
_RESULTS: dict[str, dict] = {}
_RESULT_TTL_SEC = 60 * 10


def _store_result(
    token: str,
    out_path: Path,
    tmpdir: Path,
    tries: int,
    best_score_100: float,
) -> None:
    _RESULTS[token] = {
        "out_path": out_path,
        "tmpdir": tmpdir,
        "tries": tries,
        "best_score_100": best_score_100,
        "ts": time.time(),
        "status": "done",
        "progress": 1.0,
    }


def _pop_result(token: str) -> dict | None:
    data = _RESULTS.pop(token, None)
    return data


def _get_result(token: str) -> dict | None:
    return _RESULTS.get(token)


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


@app.get("/", response_class=HTMLResponse)
def home():
    # 一個最簡單的上傳頁，不用任何前端框架
    return """
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Airport Scheduler</title>
      </head>
      <body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;">
        <h2>Upload Excel → Run → Download Excel</h2>
        <p>
          <a href="https://drive.google.com/drive/folders/1mNXtRv5olbJQAGhnVy30mBoa8m4nTAJT?usp=sharing" target="_blank" rel="noopener noreferrer">
            下載模板
          </a>
        </p>

        <form action="/run" method="post" enctype="multipart/form-data">
          <p>
            Excel file (.xlsx):
            <input type="file" name="file" accept=".xlsx" required />
          </p>

          <p>
            優先次序:
            <select name="priority_mode" id="priorityMode">
              <option value="team1">一分隊</option>
              <option value="team2">二分隊</option>
              <option value="team3">三分隊</option>
              <option value="custom">客製化</option>
            </select>
          </p>

          <div id="customWrap" style="display: none; border: 1px solid #ddd; padding: 10px; margin-bottom: 10px;">
            <div style="margin-bottom: 6px;">自訂模組優先順序（由上到下）</div>
            <ul id="customList" style="list-style: none; padding: 0; margin: 0;">
              <li data-key="fairness" style="display: flex; align-items: center; gap: 8px; margin-bottom: 6px;">
                <span style="width: 140px;">職務次數平均</span>
                <button type="button" class="upBtn">↑</button>
                <button type="button" class="downBtn">↓</button>
              </li>
              <li data-key="shift_count" style="display: flex; align-items: center; gap: 8px;">
                <span style="width: 140px;">班段次數平均</span>
                <button type="button" class="upBtn">↑</button>
                <button type="button" class="downBtn">↓</button>
              </li>
            </ul>
          </div>

          <input type="hidden" name="custom_order" id="customOrder" value="fairness,shift_count" />

          <button type="submit">Run</button>
        </form>
        <script>
          const modeSel = document.getElementById('priorityMode');
          const customWrap = document.getElementById('customWrap');
          const customList = document.getElementById('customList');
          const customOrder = document.getElementById('customOrder');

          function syncOrder() {
            const keys = Array.from(customList.querySelectorAll('li')).map(li => li.dataset.key);
            customOrder.value = keys.join(',');
          }

          modeSel.addEventListener('change', () => {
            customWrap.style.display = (modeSel.value === 'custom') ? 'block' : 'none';
          });

          customList.addEventListener('click', (e) => {
            if (!(e.target instanceof HTMLButtonElement)) return;
            const li = e.target.closest('li');
            if (!li) return;
            if (e.target.classList.contains('upBtn')) {
              const prev = li.previousElementSibling;
              if (prev) customList.insertBefore(li, prev);
            } else if (e.target.classList.contains('downBtn')) {
              const next = li.nextElementSibling;
              if (next) customList.insertBefore(next, li);
            }
            syncOrder();
          });
        </script>
      </body>
    </html>
    """


@app.post("/run")
async def run(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    priority_mode: str = Form("team1"),
    custom_order: str = Form("fairness,shift_count"),
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
                )

                tries_used = int(result.get("tries", 0) or 0)
                best_score = float(result.get("best_score_100", 0.0) or 0.0)
                _store_result(token, out_path, tmpdir, tries_used, best_score)
            except Exception as e:
                _set_error(token, str(e))

        background_tasks.add_task(_run_job)

        return HTMLResponse(
            f"""
            <html>
              <head>
                <meta charset="utf-8" />
                <title>Airport Scheduler</title>
              </head>
              <body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;">
                <h2>Running...</h2>
                <div style="width: 100%; height: 12px; border: 1px solid #999; background: #f2f2f2;">
                  <div id="progressBar" style="height: 100%; width: 0%; background: #4a90e2;"></div>
                </div>
                <div id="progressText" style="margin-top: 6px; font-size: 12px; color: #555;"></div>
                <div id="errorText" style="margin-top: 12px; color: #b00020;"></div>
                <script>
                  const bar = document.getElementById('progressBar');
                  const txt = document.getElementById('progressText');
                  const err = document.getElementById('errorText');
                  async function poll() {{
                    const resp = await fetch('/progress/{token}');
                    const data = await resp.json();
                    if (data.status === 'error') {{
                      err.textContent = data.message || 'Run failed.';
                      return;
                    }}
                    const pct = Math.floor((data.progress || 0) * 100);
                    bar.style.width = pct + '%';
                    const eta = data.eta_sec;
                    let etaText = '';
                    if (eta !== null && eta !== undefined) {{
                      const total = Math.max(0, Math.floor(eta));
                      const mm = Math.floor(total / 60);
                      const ss = total % 60;
                      etaText = ' | ETA ' + mm + 'm ' + ss + 's';
                    }}
                    txt.textContent = 'Progress ' + pct + '%'+ etaText;
                    if (data.status === 'done') {{
                      window.location.href = '/preview/{token}';
                      return;
                    }}
                    setTimeout(poll, 500);
                  }}
                  poll();
                </script>
              </body>
            </html>
            """
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
def preview(token: str):
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

    max_rows = ws.max_row
    max_cols = ws.max_column

    # Build HTML table with basic fill colors + bold
    rows_html = []
    for r in range(1, max_rows + 1):
        cells = []
        for c in range(1, max_cols + 1):
            cell = ws.cell(row=r, column=c)
            val = "" if cell.value is None else str(cell.value)
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
            style_attr = f' style="{" ".join(styles)}"' if styles else ""
            tag = "th" if r == 1 else "td"
            cells.append(f"<{tag}{style_attr}>{val}</{tag}>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    table_html = "<table border='1' cellspacing='0' cellpadding='4'>" + "".join(rows_html) + "</table>"

    return HTMLResponse(
        f"""
        <html>
          <head>
            <meta charset="utf-8" />
            <title>Preview</title>
          </head>
          <body style="font-family: sans-serif; max-width: 1000px; margin: 24px auto;">
            <h2>預覽（完整班表）</h2>
            {table_html}
            <p><a href="/download/{token}">下載結果 Excel</a></p>
            <p><a href="/">回首頁</a></p>
          </body>
        </html>
        """
    )
