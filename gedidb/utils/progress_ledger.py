# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import csv, os, time, html
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime


@dataclass
class Row:
    granule_id: str
    timeframe: str
    submitted_ts: float
    started_ts: float
    finished_ts: float
    duration_s: float
    status: str  # 'ok' | 'fail'
    n_records: Optional[int] = None
    bytes_downloaded: Optional[int] = None
    products: Optional[str] = None
    error_msg: Optional[str] = None


class ProgressLedger:
    def __init__(self, out_dir: str, timeframe: str):
        self.out_dir = out_dir
        self.timeframe = timeframe
        os.makedirs(self.out_dir, exist_ok=True)
        os.makedirs(os.path.join(self.out_dir, "errors"), exist_ok=True)
        self.csv_path = os.path.join(self.out_dir, "ledger.csv")
        self.md_path = os.path.join(self.out_dir, "STATUS.md")
        self.html_path = os.path.join(self.out_dir, "report.html")
        self._rows: List[Row] = []
        self._submits: Dict[str, float] = {}
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(
                    [
                        "granule_id",
                        "timeframe",
                        "submitted_ts",
                        "started_ts",
                        "finished_ts",
                        "duration_s",
                        "status",
                        "n_records",
                        "bytes_downloaded",
                        "products",
                        "error_msg",
                    ]
                )

    def note_submit(self, granule_id: str):
        self._submits[granule_id] = time.time()

    def append(self, r: Row):
        self._rows.append(r)
        with open(self.csv_path, "a", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    r.granule_id,
                    r.timeframe,
                    int(r.submitted_ts),
                    int(r.started_ts),
                    int(r.finished_ts),
                    round(r.duration_s, 3),
                    r.status,
                    r.n_records if r.n_records is not None else "",
                    r.bytes_downloaded if r.bytes_downloaded is not None else "",
                    r.products or "",
                    r.error_msg or "",
                ]
            )

    def write_status_md(self):
        total = self.total
        ok = self.ok_count
        fail = self.fail_count
        eta_min = self.eta_minutes()
        last5 = self._rows[-5:]
        lines = []
        lines.append(f"# GEDI Processing — {self.timeframe}")
        lines.append("")
        lines.append(f"- **Granules processed**: {ok+fail}/{total if total else '?'}")
        lines.append(f"- **Success**: {ok}   **Failed**: {fail}")
        if eta_min is not None:
            lines.append(f"- **ETA**: ~{eta_min:.1f} minutes")
        lines.append(
            f"- **Last update**: {datetime.now().isoformat(timespec='seconds')}"
        )
        lines.append("")
        if last5:
            lines.append("## Last 5")
            lines.append("| granule_id | status | duration_s | n_records |")
            lines.append("|---|---|---:|---:|")
            for r in last5:
                lines.append(
                    f"| {r.granule_id} | {r.status} | {r.duration_s:.1f} | {r.n_records or ''} |"
                )
        with open(self.md_path, "w") as f:
            f.write("\n".join(lines))

    def write_html(self):
        # Simple static HTML; no external JS/CSS
        ok = self.ok_count
        fail = self.fail_count
        done = ok + fail
        total = self.total or 0
        pct = (100.0 * done / total) if total else 0.0
        rows_html = "\n".join(
            [
                f"<tr><td>{html.escape(r.granule_id)}</td><td>{r.status}</td>"
                f"<td>{r.duration_s:.1f}</td><td>{r.n_records or ''}</td>"
                f"<td>{html.escape(r.error_msg or '')}</td></tr>"
                for r in self._rows[-200:]  # tail for brevity
            ]
        )
        doc = f"""<!doctype html><html><head><meta charset="utf-8">
<title>GEDI report — {html.escape(self.timeframe)}</title>
<style>
body{{font-family:system-ui,Arial,sans-serif; margin:24px;}}
h1,h2{{margin: 0.2em 0;}}
.card{{border:1px solid #ddd;border-radius:12px;padding:16px;margin-bottom:16px;}}
table{{border-collapse:collapse;width:100%;}}
th,td{{border:1px solid #ddd;padding:6px 8px;font-size:14px;}}
.progress{{height:14px;background:#eee;border-radius:7px;overflow:hidden}}
.bar{{height:100%;width:{pct:.2f}%;background:#7aa8;}}
.meta{{color:#555;font-size:14px}}
</style></head><body>
<h1>GEDI Granules — {html.escape(self.timeframe)}</h1>
<div class="meta">Last update: {html.escape(datetime.now().isoformat(timespec='seconds'))}</div>

<div class="card">
  <h2>Summary</h2>
  <div class="progress"><div class="bar"></div></div>
  <p class="meta">{done}/{total} processed — {ok} ok, {fail} failed</p>
</div>

<div class="card">
  <h2>Recent (tail)</h2>
  <table>
    <thead><tr><th>granule_id</th><th>status</th><th>duration_s</th><th>n_records</th><th>error</th></tr></thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</div>
</body></html>"""
        with open(self.html_path, "w") as f:
            f.write(doc)

    @property
    def ok_count(self):
        return sum(r.status == "ok" for r in self._rows)

    @property
    def fail_count(self):
        return sum(r.status == "fail" for r in self._rows)

    @property
    def total(self):  # unknown at start; we infer from assigned submits + rows
        # Best effort: submitted tasks are the intended total for this timeframe
        return max(len(self._submits), self.ok_count + self.fail_count)

    def eta_minutes(self):
        done = self.ok_count + self.fail_count
        if done == 0:
            return None
        first_ts = self._rows[0].started_ts if self._rows else None
        if first_ts is None:
            return None
        elapsed = time.time() - first_ts
        remain = self.total - done
        rate = done / max(elapsed, 1e-6)
        return (remain / rate) / 60.0 if rate > 0 else None

    def write_error(self, granule_id: str, traceback_str: str):
        p = os.path.join(self.out_dir, "errors", f"{granule_id}.log")
        with open(p, "w") as f:
            f.write(traceback_str)
