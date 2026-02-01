from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
import time
import uuid
import pandas as pd
from openpyxl import load_workbook

from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse

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
    best_score: float,
) -> None:
    _RESULTS[token] = {
        "out_path": out_path,
        "tmpdir": tmpdir,
        "tries": tries,
        "best_std": best_score,
        "ts": time.time(),
    }


def _pop_result(token: str) -> dict | None:
    data = _RESULTS.pop(token, None)
    return data


def _get_result(token: str) -> dict | None:
    return _RESULTS.get(token)


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
          <a href="https://docs.google.com/spreadsheets/d/1pzWkA2eVotA6G4fKjOdjmmL9v75zQJDkrq-v5vkRzKc/edit?usp=sharing" target="_blank" rel="noopener noreferrer">
            下載模板
          </a>
        </p>

        <form id="runForm" action="/run" method="post" enctype="multipart/form-data">
          <p>
            Excel file (.xlsx):
            <input type="file" name="file" accept=".xlsx" required />
          </p>

          <p>
            Days:
            <input type="number" name="days" value="28" min="1" max="31" />
          </p>

          <p>
            Min tries:
            <input type="range" name="min_tries" value="700" min="700" max="5000" step="10"
                   oninput="document.getElementById('minTriesVal').textContent=this.value" />
            <span id="minTriesVal">700</span>
          </p>

          <p>
            Patience:
            <input type="number" name="patience" value="10" min="1" max="200" />
          </p>

          <button type="submit">Run</button>
        </form>
        <div id="progressWrap" style="margin-top: 20px; display: none;">
          <div style="margin-bottom: 6px;">Running...</div>
          <div style="width: 100%; height: 12px; border: 1px solid #999; background: #f2f2f2;">
            <div id="progressBar" style="height: 100%; width: 0%; background: #4a90e2;"></div>
          </div>
          <div id="progressText" style="margin-top: 6px; font-size: 12px; color: #555;"></div>
        </div>
        <div id="errorText" style="margin-top: 12px; color: #b00020;"></div>
        <script>
          const form = document.getElementById('runForm');
          const wrap = document.getElementById('progressWrap');
          const bar = document.getElementById('progressBar');
          const txt = document.getElementById('progressText');
          const err = document.getElementById('errorText');
          form.addEventListener('submit', async (e) => {
            e.preventDefault();
            err.textContent = '';
            wrap.style.display = 'block';
            bar.style.width = '0%';
            const minTries = parseInt(form.min_tries.value || '700', 10);
            const durationMs = Math.max(1, minTries) * 0.01 * 1000;
            const start = performance.now();
            const tick = (now) => {
              const t = Math.min(1, (now - start) / durationMs);
              bar.style.width = Math.floor(t * 100) + '%';
              txt.textContent = 'Estimated time: ' + (durationMs / 1000).toFixed(2) + 's';
              if (t < 1) requestAnimationFrame(tick);
            };
            requestAnimationFrame(tick);
            try {
              const resp = await fetch(form.action, { method: 'POST', body: new FormData(form) });
              const html = await resp.text();
              document.open();
              document.write(html);
              document.close();
            } catch (e2) {
              err.textContent = 'Run failed: ' + e2;
            }
          });
        </script>
      </body>
    </html>
    """


@app.post("/run")
async def run(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    days: int = Form(28),
    min_tries: int = Form(700),
    patience: int = Form(10),
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

        # 5) 跑排班
        try:
            result = run_scheduler(
                input_excel_path=str(in_path),
                output_excel_path=str(out_path),
                days_limit=int(days),
                search_best_roster=True,
                search_max_tries=5000,
                search_min_tries=max(700, int(min_tries)),
                search_patience=int(patience),
                require_all_pulls_nonzero=False,
                debug=False,
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        # 6) 產生下載頁
        token = uuid.uuid4().hex
        tries_used = int(result.get("tries", 0) or 0)
        best_score = float(result.get("best_std", 0.0) or 0.0)
        best_pull_std = float(result.get("best_pull_std", 0.0) or 0.0)
        best_fair_std = float(result.get("best_fair_std", 0.0) or 0.0)
        _store_result(token, out_path, tmpdir, tries_used, best_score)

        return HTMLResponse(
            f"""
            <html>
              <head>
                <meta charset="utf-8" />
                <title>Airport Scheduler</title>
              </head>
              <body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;">
                <h2>完成</h2>
                <p>Tries: {tries_used}</p>
                <p>Best score (pull std + fairness std): {best_score:.4f}</p>
                <p>Pull std: {best_pull_std:.4f}</p>
                <p>Fairness std: {best_fair_std:.4f}</p>
                <p><a href="/download/{token}">下載結果 Excel</a></p>
                <p><a href="/preview/{token}">預覽結果（含顏色）</a></p>
                <div style="border: 1px solid #ddd; padding: 12px; overflow: auto;">
                  <iframe src="/preview/{token}" style="width: 100%; height: 520px; border: 0;"></iframe>
                </div>
                <p><a href="/">回首頁</a></p>
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
