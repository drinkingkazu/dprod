"""Static monitoring page for a campaign (controller side, stdlib only).

    dprod web [--out DIR] [--publish]        # or: dprod watch --web

Writes <out>/index.html (self-contained: data, CSS and JS inline, no external
resources) and <out>/status.json (the same data). When served over http(s), an
open page re-fetches status.json every minute or two and redraws in place, so a
browser left open follows a running `dprod watch --web`.

Site config:
    web:
      dir: /path/served/by/a/web/server/{campaign}   # default <campaign dir>/web
      publish: "rsync -a {dir}/ host:/var/www/{campaign}/"   # optional, run after writing
      refresh_s: 600                                  # expected update interval (staleness badge)
"""

import json
import os
import shlex
import subprocess
import time

from . import db as D
from . import layout as L

from .webdata import STATES, stage_stats, write_plan  # noqa: F401


def collect(c):
    """Everything the page shows, as a JSON-serializable dict."""
    con = c.con
    root = c.cfg["root_stage"]
    rs = c.cfg["stages"][root]
    n_root = con.execute("SELECT COUNT(*) FROM tasks WHERE stage = ?", (root,)).fetchone()[0]
    planned_events = n_root * int(rs["events_per_job"] or 0)
    stages = []
    for name, s in c.cfg["stages"].items():
        counts = {k: 0 for k in STATES}
        for r in con.execute("SELECT status, COUNT(*) n FROM tasks WHERE stage = ? GROUP BY status",
                             (name,)):
            counts[r["status"]] = r["n"]
        # queued vs running comes from the attempts (a task's status is 'submitted' or 'running')
        done = con.execute(
            "SELECT a.task_id, a.end_time t, a.wall_s, a.elapsed_s, a.max_rss_mb, a.avg_rss_mb,"
            " a.gpu_util_pct, a.gpu_mem_used_mb, t.n_events FROM attempts a JOIN tasks t"
            " ON t.stage = a.stage AND t.task_id = a.task_id AND t.n_attempts = a.attempt"
            " WHERE a.stage = ? AND a.state = 'done' AND t.status = 'done'", (name,)).fetchall()
        sizes = {r[0]: r[1] for r in con.execute(
            "SELECT task_id, SUM(size) FROM files WHERE stage = ? GROUP BY task_id", (name,))}
        stats = stage_stats([{"t": r["t"], "wall": r["wall_s"] or r["elapsed_s"],
                              "events": r["n_events"], "bytes": sizes.get(r["task_id"]),
                              "max_rss": r["max_rss_mb"],
                              "avg_rss": r["avg_rss_mb"], "gpu_util": r["gpu_util_pct"],
                              "gpu_mem": r["gpu_mem_used_mb"]} for r in done])
        n_att = con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ?", (name,)).fetchone()[0]
        n_fail = con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state = 'failed'",
                             (name,)).fetchone()[0]
        stage = {
            "name": name, "alias": s["alias"] or name, "parent": s["parent"],
            "external": s.get("external"),
            "enabled": s["enabled"], "merge": s["merge"],
            "tasks": sum(counts.values()), "counts": counts,
            "planned_events": planned_events,
            "bytes": con.execute("SELECT COALESCE(SUM(size), 0) FROM files WHERE stage = ?",
                                 (name,)).fetchone()[0],
            "attempts": n_att, "failed_attempts": n_fail,
        }
        stage.update(stats)
        stages.append(stage)
    failures = []
    for r in con.execute(
            "SELECT a.*, s.array_job_id FROM attempts a JOIN submissions s ON s.id = a.submission_id"
            " WHERE a.state = 'failed' ORDER BY COALESCE(a.end_time, s.submit_time) DESC LIMIT 30"):
        t = con.execute("SELECT * FROM tasks WHERE stage = ? AND task_id = ?",
                        (r["stage"], r["task_id"])).fetchone()
        failures.append({
            "t": r["end_time"], "stage": r["stage"], "task_id": r["task_id"],
            "name": L.task_name(r["stage"], t["first_job"], t["last_job"]),
            "attempt": r["attempt"], "slurm": "%s_%s" % (r["array_job_id"], r["array_index"]),
            "node": r["node"] or "", "sched_state": r["sched_state"] or "",
            "task_status": t["status"],
            "reason": (r["reason"] or "").strip().splitlines()[0][:300] if r["reason"] else ""})
    web = c.site.get("web") or {}
    return {
        "source": "controller",
        "campaign": c.tag, "site": c.site["name"], "description": c.cfg.get("description", ""),
        "generated": time.time(), "refresh_s": int(web.get("refresh_s", 600)),
        "max_attempts": c.cfg["max_attempts"], "planned_jobs": n_root,
        "planned_events": planned_events,
        "active": {st["name"]: {
            "queued": con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state = 'submitted'",
                                  (st["name"],)).fetchone()[0],
            "running": con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state = 'running'",
                                   (st["name"],)).fetchone()[0]} for st in stages},
        "stages": stages, "failures": failures,
    }


def _expand(d):
    return os.path.expanduser(os.path.expandvars(d))


def web_base(site, override=None):
    """Directory holding campaigns.json and the all-campaigns page: the part of
    web.dir before {campaign}, web.dir itself if it has no {campaign}, or the
    storage root by default (pages then live in <campaign>/web)."""
    if override:
        return os.path.dirname(os.path.abspath(_expand(override)))
    d = (site.get("web") or {}).get("dir")
    if not d:
        return site["storage_root"]
    d = _expand(d)
    if "{campaign}" in d:
        return d.split("{campaign}")[0].rstrip("/") or "/"
    return d


def web_dir(c, override=None):
    """This campaign's page directory."""
    if override:
        return os.path.abspath(_expand(override))
    d = (c.site.get("web") or {}).get("dir")
    if not d:
        return os.path.join(c.dir, "web")
    d = _expand(d)
    if "{campaign}" in d:
        return d.format(campaign=c.tag)
    return os.path.join(d, c.tag)          # shared web dir: one subdirectory per campaign


def _write(path, text):
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def _embed(template, obj):
    return template.replace("__DATA__", json.dumps(obj, separators=(",", ":"), default=str)
                            .replace("</", "<\\/"))


def write_files(d, data, json_name, base=None):
    """Write <d>/<json_name> and <d>/index.html (with `data` embedded, for viewing
    as a local file). Used by the controller (status.json) and by jobs (jobs.json).
    With `base`, also register the campaign in <base>/campaigns.json and rewrite the
    all-campaigns page <base>/index.html."""
    from .webdata import update_registry
    os.makedirs(d, exist_ok=True)
    _write(os.path.join(d, json_name), json.dumps(data, separators=(",", ":"), default=str))
    page = dict(data)
    if base:
        reg = update_registry(base, data["campaign"], d, data)
        page["to_base"] = os.path.relpath(base, d)
        page["registry"] = list(reg["campaigns"].values())
        if os.path.realpath(base) != os.path.realpath(d):
            write_hub(base, reg)
    _write(os.path.join(d, "index.html"), _embed(PAGE, page))
    return os.path.join(d, "index.html")


def write_hub(base, reg):
    _write(os.path.join(base, "index.html"), _embed(HUB, reg))


def write(c, out=None, publish=False, log=print):
    write_plan(c)
    data = collect(c)
    d = web_dir(c, out)
    path = write_files(d, data, "status.json", base=web_base(c.site, out))
    cmd = (c.site.get("web") or {}).get("publish")
    if publish and cmd:
        cmd = cmd.format(dir=shlex.quote(d), campaign=c.tag)
        rc = subprocess.call(cmd, shell=True)
        log("published (%s): exit code %d" % (cmd, rc))
    return path


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DORAEMON production monitor</title>
<style>
.viz-root {
  color-scheme: light;
  --page: #f9f9f7; --surface-1: #fcfcfb;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --s4: #eda100; --s5: #e87ba4;
  --st-running: #2a78d6; --st-queued: #86b6ef; --st-new: #e1e0d9;
  --good: #0ca30c; --warning: #fab219; --serious: #ec835a; --critical: #d03b3b;
  --hist: #2a78d6;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    color-scheme: dark;
    --page: #0d0d0d; --surface-1: #1a1a19;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500; --s5: #d55181;
    --st-running: #3987e5; --st-queued: #184f95; --st-new: #2c2c2a; --hist: #3987e5;
  }
}
:root[data-theme="dark"] .viz-root {
  color-scheme: dark;
  --page: #0d0d0d; --surface-1: #1a1a19;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
  --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
  --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500; --s5: #d55181;
  --st-running: #3987e5; --st-queued: #184f95; --st-new: #2c2c2a; --hist: #3987e5;
}
* { box-sizing: border-box; }
body { margin: 0; }
.viz-root { background: var(--page); color: var(--text-primary); min-height: 100vh;
  font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; padding: 24px 32px 48px; }
header { display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
h1 { font-size: 20px; font-weight: 600; margin: 0; }
h2 { font-size: 15px; font-weight: 600; margin: 0 0 4px; }
.sub { color: var(--text-secondary); }
.muted { color: var(--text-muted); }
.spacer { flex: 1; }
.campnav { display: inline-flex; align-items: center; gap: 8px; font-size: 13px; color: var(--text-secondary); }
.campnav select { font: inherit; color: var(--text-primary); background: var(--surface-1);
  border: 1px solid var(--border); border-radius: 6px; padding: 3px 6px; }
.campnav a { color: var(--text-secondary); }
.seg-ctl { display: inline-flex; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
.seg-ctl button { font: inherit; font-size: 13px; color: var(--text-secondary); background: var(--surface-1);
  border: 0; padding: 4px 10px; cursor: pointer; }
.seg-ctl button + button { border-left: 1px solid var(--border); }
.seg-ctl button[aria-pressed="true"] { color: var(--text-primary); font-weight: 600; background: var(--page); }
.seg-ctl button:disabled { color: var(--text-muted); cursor: default; }
.srcnote { color: var(--text-secondary); font-size: 13px; margin: -8px 0 16px; }
button.theme { font: inherit; color: var(--text-secondary); background: var(--surface-1);
  border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px; cursor: pointer; }
.card { background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
  padding: 18px 20px; margin-bottom: 20px; }
.card .desc { color: var(--text-secondary); margin: 0 0 14px; font-size: 13px; }
.kpis { display: grid; grid-template-columns: 2fr repeat(4, 1fr); gap: 20px; margin-bottom: 20px; }
.kpi { background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px; }
.kpi .label { color: var(--text-secondary); font-size: 13px; }
.kpi .value { font-size: 28px; font-weight: 600; margin-top: 4px; }
.kpi.hero .value { font-size: 52px; line-height: 1.1; }
.kpi .note { color: var(--text-muted); font-size: 12px; margin-top: 2px; }
.badge { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; color: var(--text-secondary); }
.badge .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
.legend { display: flex; flex-wrap: wrap; gap: 6px 18px; margin: 0 0 12px; font-size: 13px; color: var(--text-secondary); }
.legend .key { display: inline-flex; align-items: center; gap: 6px; }
.legend .sw { width: 12px; height: 12px; border-radius: 3px; display: inline-block; }
.legend .ln { width: 16px; height: 2px; border-radius: 1px; display: inline-block; }
.legend .ic { font-size: 11px; width: 12px; text-align: center; color: var(--text-secondary); }
svg { display: block; overflow: visible; }
svg text { fill: var(--text-secondary); font-size: 12px; }
svg .axis text { fill: var(--text-muted); font-variant-numeric: tabular-nums; }
.stagerow text.name { fill: var(--text-primary); font-weight: 600; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th { text-align: left; font-weight: 600; color: var(--text-secondary); border-bottom: 1px solid var(--axis); padding: 6px 10px 6px 0; white-space: nowrap; }
td { border-bottom: 1px solid var(--grid); padding: 6px 10px 6px 0; vertical-align: top; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
td.reason { color: var(--text-secondary); max-width: 520px; word-break: break-word; }
.status { display: inline-flex; gap: 4px; align-items: center; white-space: nowrap; }
details summary { cursor: pointer; color: var(--text-secondary); font-size: 13px; margin-top: 10px; }
.multiples { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 16px 28px; }
.multiples h3 { font-size: 13px; font-weight: 600; margin: 0 0 2px; }
#tip { position: fixed; pointer-events: none; background: var(--surface-1); color: var(--text-primary);
  border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; font-size: 12px;
  box-shadow: 0 4px 16px rgba(0,0,0,0.12); display: none; z-index: 10; min-width: 120px; }
#tip .t { color: var(--text-secondary); margin-bottom: 4px; }
#tip .row { display: flex; align-items: center; gap: 8px; }
#tip .row b { font-weight: 600; min-width: 48px; text-align: right; font-variant-numeric: tabular-nums; }
#tip .row .k { width: 12px; height: 2px; border-radius: 1px; display: inline-block; }
.hit { fill: transparent; cursor: default; }
.hit:hover + .seg, .seg.hover { filter: brightness(1.12); }
.empty { color: var(--text-muted); font-size: 13px; padding: 12px 0; }
@media (max-width: 900px) { .kpis { grid-template-columns: 1fr 1fr; } .kpi.hero { grid-column: 1 / -1; } }
</style>
</head>
<body>
<div class="viz-root" id="root">
  <header>
    <h1 id="title">DORAEMON production</h1>
    <span class="sub" id="subtitle"></span>
    <span class="campnav" id="campnav" hidden>
      <label for="campsel">Campaign</label>
      <select id="campsel"></select>
      <a id="alllink" href="#">All campaigns</a>
    </span>
    <span class="spacer"></span>
    <span class="badge" id="freshness"></span>
    <span class="seg-ctl" id="srcctl" role="group" aria-label="Data source">
      <button type="button" data-src="auto">Auto</button><button type="button" data-src="controller">Live</button><button type="button" data-src="jobs">Job records</button>
    </span>
    <button class="theme" id="themebtn" type="button">Theme</button>
  </header>
  <div class="srcnote" id="srcnote"></div>
  <div class="kpis" id="kpis"></div>
  <section class="card">
    <h2>Progress by stage</h2>
    <p class="desc">Tasks per stage; one task is one slurm array element (stage 2 and 3 tasks process several stage-1 jobs).</p>
    <div class="legend" id="stlegend"></div>
    <div id="stagebars"></div>
  </section>
  <section class="card">
    <h2>Events produced over time</h2>
    <p class="desc">Cumulative events in successfully finished tasks, by stage.</p>
    <div class="legend" id="cumlegend"></div>
    <div id="cumchart"></div>
    <details><summary>Table view</summary><div id="cumtable"></div></details>
  </section>
  <section class="card">
    <h2>Statistics of successful tasks</h2>
    <p class="desc">Wall time, memory and GPU use are averaged over finished tasks; failure rate is failed attempts over all attempts.</p>
    <div id="stattable"></div>
  </section>
  <section class="card">
    <h2>Wall time per task</h2>
    <p class="desc">Distribution over successful tasks, one panel per stage.</p>
    <div class="multiples" id="hists"></div>
  </section>
  <section class="card">
    <h2>Output data size per task</h2>
    <p class="desc">Total size of all output files of a task, over successful tasks.</p>
    <div class="multiples" id="hsize"></div>
  </section>
  <section class="card">
    <h2>Peak RAM per task</h2>
    <p class="desc">Peak resident memory of the job (process tree), over successful tasks.</p>
    <div class="multiples" id="hram"></div>
  </section>
  <section class="card">
    <h2>GPU memory per task</h2>
    <p class="desc">GPU memory used, time-averaged over the job (GPU stages only). JAX reserves most of the card up front by default.</p>
    <div class="multiples" id="hgpumem"></div>
  </section>
  <section class="card">
    <h2>GPU utilization per task</h2>
    <p class="desc">GPU utilization, time-averaged over the job (GPU stages only).</p>
    <div class="multiples" id="hgpuutil"></div>
  </section>
  <section class="card">
    <h2>Recent failures</h2>
    <p class="desc">Latest failed attempts, newest first. <span class="muted">dprod failures &lt;stage&gt; -v</span> shows full reasons and log paths.</p>
    <div id="failtable"></div>
  </section>
  <div id="tip" role="status" aria-live="polite"></div>
</div>
<script type="application/json" id="data">__DATA__</script>
<script>
(function () {
  "use strict";
  var EMB = JSON.parse(document.getElementById("data").textContent);
  var SRC = {controller: null, jobs: null};   // latest data from each source
  SRC[EMB.source || "controller"] = EMB;
  var choice = "auto";
  try { choice = localStorage.getItem("dprod-src") || "auto"; } catch (e) {}
  function pick() {
    var c = SRC.controller, j = SRC.jobs;
    if (choice === "controller" && c) return c;
    if (choice === "jobs" && j) return j;
    if (c && j) return c.generated >= j.generated ? c : j;
    return c || j || EMB;
  }
  var D = pick();
  var SVGNS = "http://www.w3.org/2000/svg";
  var SERIES = ["--s1", "--s2", "--s3", "--s4", "--s5"];
  var STATE = [
    {k: "done", label: "Done", color: "--good", icon: "✓"},
    {k: "running", label: "Running", color: "--st-running"},
    {k: "submitted", label: "Queued", color: "--st-queued"},
    {k: "failed", label: "Failed", color: "--critical", icon: "✕"},
    {k: "lost", label: "Lost (no record)", color: "--warning", icon: "?"},
    {k: "abandoned", label: "Abandoned", color: "--serious", icon: "!"},
    {k: "new", label: "Not submitted", color: "--st-new"}
  ];
  // ---- helpers
  function el(tag, attrs, parent) {
    var e = document.createElementNS(SVGNS, tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }
  function h(tag, cls, text, parent) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    if (parent) parent.appendChild(e);
    return e;
  }
  function css(v) { return getComputedStyle(document.getElementById("root")).getPropertyValue(v).trim(); }
  function fmt(n) {
    if (n === null || n === undefined) return "–";
    var a = Math.abs(n);
    if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + "B";
    if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + "M";
    if (a >= 1e4) return (n / 1e3).toFixed(a >= 1e5 ? 0 : 1) + "K";
    return Math.round(n).toLocaleString();
  }
  function fmtBytes(b) {
    var u = ["B", "KB", "MB", "GB", "TB", "PB"], i = 0;
    while (b >= 1000 && i < u.length - 1) { b /= 1000; i++; }
    return (i ? b.toFixed(b >= 100 ? 0 : 1) : b) + " " + u[i];
  }
  function fmtDur(s) {
    if (s === null || s === undefined) return "–";
    if (s < 120) return Math.round(s) + " s";
    if (s < 7200) return (s / 60).toFixed(1) + " min";
    return (s / 3600).toFixed(2) + " h";
  }
  function fmtMB(m) { return m === null || m === undefined ? "–" : (m >= 1024 ? (m / 1024).toFixed(1) + " GB" : Math.round(m) + " MB"); }
  function fmtTime(t) {
    var d = new Date(t * 1000);
    return d.toLocaleString(undefined, {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit"});
  }
  function niceTicks(max, n, integer) {
    if (max <= 0) return [0, 1];
    var step = Math.pow(10, Math.floor(Math.log10(max / n)));
    var err = max / n / step;
    step *= err >= 7.5 ? 10 : err >= 3.5 ? 5 : err >= 1.5 ? 2 : 1;
    if (integer) step = Math.max(1, Math.round(step));
    var t = [];
    for (var v = 0; v <= max + step * 0.5; v += step) t.push(v);
    if (t[t.length - 1] < max) t.push(t[t.length - 1] + step);
    return t;
  }
  var tip = document.getElementById("tip");
  function showTip(evt, title, rows) {
    tip.textContent = "";
    if (title) h("div", "t", title, tip);
    rows.forEach(function (r) {
      var row = h("div", "row", null, tip);
      if (r.color) { var k = h("span", "k", null, row); k.style.background = r.color; }
      h("b", null, r.value, row);
      h("span", null, r.label, row);
    });
    tip.style.display = "block";
    var x = evt.clientX + 14, y = evt.clientY + 14;
    var w = tip.offsetWidth, hh = tip.offsetHeight;
    if (x + w > window.innerWidth - 8) x = evt.clientX - w - 14;
    if (y + hh > window.innerHeight - 8) y = evt.clientY - hh - 14;
    tip.style.left = x + "px"; tip.style.top = y + "px";
  }
  function hideTip() { tip.style.display = "none"; }
  function stageColor(i) { return css(SERIES[i % SERIES.length]); }

  // ---- header
  function ago(t) {
    if (!t) return "never";
    var m = (Date.now() / 1000 - t) / 60;
    return m < 1 ? "just now" : m < 120 ? Math.round(m) + " min ago" :
           m < 2880 ? (m / 60).toFixed(1) + " h ago" : Math.round(m / 1440) + " days ago";
  }
  function renderHeader() {
    document.getElementById("title").textContent = "DORAEMON production · " + D.campaign;
    document.getElementById("subtitle").textContent = "site " + D.site + (D.description ? " · " + D.description : "");
    var live = D.source !== "jobs";
    var ageMin = (Date.now() / 1000 - D.generated) / 60;
    var f = document.getElementById("freshness");
    f.textContent = "";
    var dot = h("span", "dot", null, f);
    var stale = ageMin > 3 * D.refresh_s / 60 + 5;
    dot.style.background = css(stale ? "--warning" : "--good");
    h("span", null, (live ? "Live" : "Job records") + " · " + (stale ? "stale, " : "") +
      "updated " + fmtTime(D.generated) + " (" + ago(D.generated) + ")", f);
    var c = SRC.controller, j = SRC.jobs;
    document.getElementById("srcnote").textContent = live
      ? "Source: the bookkeeping database, synced with slurm by dprod." +
        (j ? " Job records were last rebuilt " + ago(j.generated) + "." : "")
      : "Source: records written by the jobs themselves (no monitoring process needed); last job record " +
        ago(D.last_record) + ", controller last synced " + (c ? ago(c.generated) : "unknown") +
        ". Queued and lost counts are estimates: a task killed without writing a record shows as running until it is past its time limit.";
    Array.prototype.forEach.call(document.querySelectorAll("#srcctl button"), function (b) {
      var src = b.getAttribute("data-src");
      b.setAttribute("aria-pressed", String(src === choice));
      b.disabled = src !== "auto" && !SRC[src];
    });
    document.title = "DORAEMON · " + D.campaign;
  }

  // ---- KPI row
  function renderKpis() {
    var box = document.getElementById("kpis"); box.textContent = "";
    var root = D.stages[0];
    var active = 0, queued = 0, failed = 0, lost = 0, bytes = 0;
    D.stages.forEach(function (s) {
      active += D.active[s.name].running; queued += D.active[s.name].queued;
      failed += s.counts.failed; lost += s.counts.lost || 0; bytes += s.bytes;
    });
    function tile(label, value, note, hero) {
      var t = h("div", "kpi" + (hero ? " hero" : ""), null, box);
      h("div", "label", label, t); h("div", "value", value, t);
      if (note) h("div", "note", note, t);
    }
    var pct = D.planned_events ? Math.round(100 * root.events / D.planned_events) : 0;
    tile("Events generated (stage " + root.alias + ")", fmt(root.events),
         "of " + fmt(D.planned_events) + " planned (" + pct + "%) in " + fmt(D.planned_jobs) + " jobs", true);
    // the last enabled stage(s) of each branch
    var en = D.stages.filter(function (s) { return s.enabled; });
    var last = en.filter(function (s) { return !en.some(function (c) { return c.parent === s.name; }); });
    tile("Fully processed events", fmt(Math.min.apply(null, last.map(function (s) { return s.events; }))),
         "through " + last.map(function (s) { return s.alias; }).join(" and "));
    tile("Running", fmt(active), fmt(queued) + " more queued");
    tile("Failed tasks", fmt(failed), (lost ? fmt(lost) + " more lost (no record) \u00b7 " : "") +
         (failed ? "see recent failures below" : lost ? "check with dprod status" : "none"));
    tile("Data on disk", fmtBytes(bytes), "all stages");
  }

  // ---- stage progress bars (stacked, part-to-whole)
  function renderStageBars() {
    var lg = document.getElementById("stlegend"); lg.textContent = "";
    STATE.forEach(function (st) {
      var k = h("span", "key", null, lg);
      var sw = h("span", "sw", null, k); sw.style.background = css(st.color);
      if (st.icon) h("span", "ic", st.icon, k);
      h("span", null, st.label, k);
    });
    var box = document.getElementById("stagebars"); box.textContent = "";
    var W = Math.max(box.clientWidth, 600), rowH = 40, labelW = 150, rightW = 190;
    var barW = W - labelW - rightW, barH = 16;
    var svg = el("svg", {width: W, height: rowH * D.stages.length, role: "img",
                         "aria-label": "Task progress by stage"}, box);
    D.stages.forEach(function (s, i) {
      var g = el("g", {class: "stagerow", transform: "translate(0," + (i * rowH + 8) + ")"}, svg);
      var name = el("text", {x: 0, y: barH - 3, class: "name"}, g);
      name.textContent = s.alias + "  " + s.name;
      if (s.external) { var ex = el("text", {x: 0, y: barH + 13, "font-size": 11}, g); ex.textContent = "from " + s.external; }
      if (!s.enabled) { var dis = el("text", {x: 0, y: barH + 13, "font-size": 11}, g); dis.textContent = "disabled"; }
      var total = Math.max(s.tasks, 1);
      var clipId = "clip" + i;
      var cp = el("clipPath", {id: clipId}, el("defs", {}, g));
      el("rect", {x: labelW, y: 0, width: barW, height: barH, rx: 4}, cp);
      var bg = el("g", {"clip-path": "url(#" + clipId + ")"}, g);
      var x = labelW, gap = 2;
      STATE.forEach(function (st) {
        var n = s.counts[st.k] || 0;
        if (!n) return;
        var w = barW * n / total;
        var seg = el("rect", {x: x, y: 0, width: Math.max(w - gap, 1), height: barH,
                              fill: css(st.color), class: "seg"}, bg);
        var hit = el("rect", {x: x, y: -8, width: Math.max(w, 6), height: barH + 16, class: "hit", tabindex: 0}, g);
        var show = function (e) {
          seg.classList.add("hover");
          showTip(e, s.alias + " " + s.name, [{value: fmt(n), label: st.label + " (" + (100 * n / total).toFixed(1) + "%)", color: css(st.color)}]);
        };
        hit.addEventListener("pointermove", show);
        hit.addEventListener("focus", function () { var r = hit.getBoundingClientRect(); show({clientX: r.left + r.width / 2, clientY: r.top}); });
        hit.addEventListener("pointerleave", function () { seg.classList.remove("hover"); hideTip(); });
        hit.addEventListener("blur", function () { seg.classList.remove("hover"); hideTip(); });
        x += w;
      });
      var t = el("text", {x: labelW + barW + 14, y: barH - 3}, g);
      t.textContent = fmt(s.counts.done) + " / " + fmt(s.tasks) + " tasks  ·  " + fmt(s.events) + " events";
    });
  }

  // ---- cumulative events (multi-line, crosshair tooltip)
  function renderCumulative() {
    var series = D.stages.map(function (s, i) { return {s: s, i: i, pts: s.cumulative}; })
                         .filter(function (x) { return x.pts.length; });
    var lg = document.getElementById("cumlegend"); lg.textContent = "";
    var box = document.getElementById("cumchart"); box.textContent = "";
    if (!series.length) { h("div", "empty", "No finished tasks yet.", box); return; }
    series.forEach(function (x) {
      var k = h("span", "key", null, lg);
      var ln = h("span", "ln", null, k); ln.style.background = stageColor(x.i);
      h("span", null, x.s.alias + " " + x.s.name, k);
    });
    var W = Math.max(box.clientWidth, 600), H = 300, m = {l: 56, r: 120, t: 12, b: 28};
    var t0 = Infinity, t1 = -Infinity, ymax = 0;
    series.forEach(function (x) {
      t0 = Math.min(t0, x.pts[0][0]); t1 = Math.max(t1, x.pts[x.pts.length - 1][0]);
      ymax = Math.max(ymax, x.pts[x.pts.length - 1][1]);
    });
    var now = D.generated; t1 = Math.max(t1, Math.min(now, t1 + 0.1 * (t1 - t0 + 60)));
    if (t1 <= t0) { t0 -= 1800; t1 += 1800; }
    var yt = niceTicks(ymax, 5, true), ytop = yt[yt.length - 1];
    var X = function (t) { return m.l + (W - m.l - m.r) * (t - t0) / (t1 - t0); };
    var Y = function (v) { return m.t + (H - m.t - m.b) * (1 - v / ytop); };
    var svg = el("svg", {width: W, height: H, role: "img", "aria-label": "Cumulative events by stage"}, box);
    var ax = el("g", {class: "axis"}, svg);
    yt.forEach(function (v) {
      el("line", {x1: m.l, x2: W - m.r, y1: Y(v), y2: Y(v), stroke: css(v === 0 ? "--axis" : "--grid"), "stroke-width": 1}, ax);
      var tx = el("text", {x: m.l - 8, y: Y(v) + 4, "text-anchor": "end"}, ax); tx.textContent = fmt(v);
    });
    var span = t1 - t0;
    var opts = span > 3 * 86400 ? {month: "short", day: "numeric"}
             : span > 86400 ? {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit"}
             : span > 1800 ? {hour: "2-digit", minute: "2-digit"}
             : {hour: "2-digit", minute: "2-digit", second: "2-digit"};
    var nx = Math.max(2, Math.floor((W - m.l - m.r) / 140)), prev = null;
    for (var i = 0; i <= nx; i++) {
      var tt = t0 + span * i / nx;
      var lab = new Date(tt * 1000).toLocaleString(undefined, opts);
      if (lab === prev) continue;
      prev = lab;
      var lx = el("text", {x: X(tt), y: H - 8, "text-anchor": i === 0 ? "start" : i === nx ? "end" : "middle"}, ax);
      lx.textContent = lab;
    }
    // step lines (the count jumps when a task finishes)
    var ends = [];
    series.forEach(function (x) {
      var d = "M" + X(x.pts[0][0]) + "," + Y(0);
      x.pts.forEach(function (p, j) {
        d += "L" + X(p[0]) + "," + Y(j ? x.pts[j - 1][1] : 0) + "L" + X(p[0]) + "," + Y(p[1]);
      });
      var last = x.pts[x.pts.length - 1];
      d += "L" + X(t1) + "," + Y(last[1]);
      el("path", {d: d, fill: "none", stroke: stageColor(x.i), "stroke-width": 2,
                  "stroke-linejoin": "round", "stroke-linecap": "round"}, svg);
      ends.push({x: X(t1), y: Y(last[1]), label: x.s.alias + "  " + fmt(last[1]), color: stageColor(x.i)});
    });
    // end markers with surface ring; labels de-collided with leader lines
    ends.sort(function (a, b) { return a.y - b.y; });
    var ly = ends.map(function (e) { return e.y; });
    for (var k = 1; k < ly.length; k++) if (ly[k] - ly[k - 1] < 14) ly[k] = ly[k - 1] + 14;
    ends.forEach(function (e, k) {
      if (Math.abs(ly[k] - e.y) > 1)
        el("line", {x1: e.x + 6, y1: e.y, x2: e.x + 14, y2: ly[k], stroke: css("--axis"), "stroke-width": 1}, svg);
      el("circle", {cx: e.x, cy: e.y, r: 4, fill: e.color, stroke: css("--surface-1"), "stroke-width": 2}, svg);
      var tx = el("text", {x: e.x + 16, y: ly[k] + 4}, svg); tx.textContent = e.label;
    });
    // crosshair
    var cross = el("line", {y1: m.t, y2: H - m.b, stroke: css("--axis"), "stroke-width": 1, visibility: "hidden"}, svg);
    var hit = el("rect", {x: m.l, y: m.t, width: W - m.l - m.r, height: H - m.t - m.b, class: "hit"}, svg);
    function valueAt(pts, t) {
      var v = 0;
      for (var j = 0; j < pts.length && pts[j][0] <= t; j++) v = pts[j][1];
      return v;
    }
    hit.addEventListener("pointermove", function (e) {
      var r = svg.getBoundingClientRect(), px = e.clientX - r.left;
      var t = t0 + (t1 - t0) * (px - m.l) / (W - m.l - m.r);
      cross.setAttribute("x1", px); cross.setAttribute("x2", px); cross.setAttribute("visibility", "visible");
      showTip(e, fmtTime(t), series.map(function (x) {
        return {value: fmt(valueAt(x.pts, t)), label: x.s.alias + " " + x.s.name, color: stageColor(x.i)};
      }));
    });
    hit.addEventListener("pointerleave", function () { cross.setAttribute("visibility", "hidden"); hideTip(); });
    // table view: events per stage at the end of each day
    var tb = document.getElementById("cumtable"); tb.textContent = "";
    var table = h("table", null, null, tb), hr = h("tr", null, null, h("thead", null, null, table));
    h("th", null, "Date", hr);
    series.forEach(function (x) { h("th", "num", x.s.alias, hr); });
    var body = h("tbody", null, null, table);
    var day = new Date(t0 * 1000); day.setHours(23, 59, 59, 0);
    for (var dt = day.getTime() / 1000; ; dt += 86400) {
      var tr = h("tr", null, null, body);
      h("td", null, new Date(Math.min(dt, now) * 1000).toLocaleDateString(), tr);
      series.forEach(function (x) { h("td", "num", fmt(valueAt(x.pts, Math.min(dt, now))), tr); });
      if (dt >= t1) break;
    }
  }

  // ---- statistics table (also the table view of the progress bars)
  function renderStats() {
    var box = document.getElementById("stattable"); box.textContent = "";
    var table = h("table", null, null, box);
    var cols = ["Stage", "Tasks", "Done", "Running", "Queued", "Failed", "Aband.", "Events",
                "Data", "Wall avg", "Wall max", "RAM avg", "RAM max", "GPU util", "GPU mem", "Failure rate"];
    var hr = h("tr", null, null, h("thead", null, null, table));
    cols.forEach(function (c, i) { h("th", i ? "num" : null, c, hr); });
    var body = h("tbody", null, null, table);
    D.stages.forEach(function (s) {
      var tr = h("tr", null, null, body);
      h("td", null, s.alias + " " + s.name + (s.enabled ? "" : " (disabled)"), tr);
      [fmt(s.tasks), fmt(s.counts.done), fmt(D.active[s.name].running), fmt(D.active[s.name].queued),
       fmt(s.counts.failed), fmt(s.counts.abandoned), fmt(s.events), fmtBytes(s.bytes),
       fmtDur(s.wall.mean), fmtDur(s.wall.max), fmtMB(s.ram_avg), fmtMB(s.ram_max),
       s.gpu_util === null ? "–" : Math.round(s.gpu_util) + "%", fmtMB(s.gpu_mem),
       s.attempts ? (100 * s.failed_attempts / s.attempts).toFixed(1) + "%" : "–"
      ].forEach(function (v) { h("td", "num", v, tr); });
    });
  }

  // ---- wall-time histograms (small multiples, one hue)
  // per-task distributions, one card per quantity, one small histogram per stage
  var HISTS = [
    {id: "hists", key: "wall", fmt: fmtDur, what: "wall time"},
    {id: "hsize", key: "bytes", fmt: function (b) { return fmtBytes(b); }, what: "output size"},
    {id: "hram", key: "max_rss", fmt: fmtMB, what: "peak RAM"},
    {id: "hgpumem", key: "gpu_mem", fmt: fmtMB, what: "GPU memory"},
    {id: "hgpuutil", key: "gpu_util", fmt: function (v) { return Math.round(v) + "%"; }, what: "GPU utilization"}
  ];
  function renderHists() {
    HISTS.forEach(function (spec) {
      var box = document.getElementById(spec.id); box.textContent = "";
      var shown = D.stages.filter(function (s) {
        var d = (s.dists || {})[spec.key];
        return d && d.hist && d.hist.length;
      });
      var card = box.parentNode;
      if (!shown.length) {
        // GPU panels only make sense once a GPU stage has finished tasks
        card.hidden = spec.key.indexOf("gpu") === 0;
        h("div", "empty", "No finished tasks yet.", box);
        return;
      }
      card.hidden = false;
      shown.forEach(function (s) { histPanel(box, s, s.dists[spec.key], spec); });
    });
  }
  function histPanel(box, s, dist, spec) {
    var hist = dist.hist;
    var cell = h("div", null, null, box);
    h("h3", null, s.alias + " " + s.name, cell);
    h("div", "muted", fmt(dist.n) + " tasks · avg " + spec.fmt(dist.avg), cell);
    var W = 260, H = 130, m = {l: 34, r: 6, t: 10, b: 22};
    var svg = el("svg", {width: W, height: H, role: "img", "aria-label": spec.what + " histogram " + s.name}, cell);
    var cmax = Math.max.apply(null, hist.map(function (b) { return b[2]; }));
    var yt = niceTicks(cmax, 3, true), ytop = yt[yt.length - 1];
    var Y = function (v) { return m.t + (H - m.t - m.b) * (1 - v / ytop); };
    var ax = el("g", {class: "axis"}, svg);
    yt.forEach(function (v) {
      el("line", {x1: m.l, x2: W - m.r, y1: Y(v), y2: Y(v), stroke: css(v === 0 ? "--axis" : "--grid"), "stroke-width": 1}, ax);
      var tx = el("text", {x: m.l - 6, y: Y(v) + 4, "text-anchor": "end"}, ax); tx.textContent = fmt(v);
    });
    var nb = hist.length, slot = (W - m.l - m.r) / nb, bw = Math.min(24, slot - 2);
    hist.forEach(function (b, j) {
      var x = m.l + j * slot + (slot - bw) / 2, y = Y(b[2]), hgt = Y(0) - y;
      if (b[2] > 0) {
        var r = Math.min(4, hgt);
        var d = "M" + x + "," + Y(0) + "V" + (y + r) + "Q" + x + "," + y + " " + (x + r) + "," + y +
                "H" + (x + bw - r) + "Q" + (x + bw) + "," + y + " " + (x + bw) + "," + (y + r) + "V" + Y(0) + "Z";
        el("path", {d: d, fill: css("--hist"), class: "seg"}, svg);
      }
      var hit = el("rect", {x: m.l + j * slot, y: m.t, width: slot, height: H - m.t - m.b, class: "hit", tabindex: 0}, svg);
      hit.addEventListener("pointermove", function (e) {
        showTip(e, spec.fmt(b[0]) + " – " + spec.fmt(b[1]), [{value: fmt(b[2]), label: "tasks"}]);
      });
      hit.addEventListener("pointerleave", hideTip);
    });
    var lo = el("text", {x: m.l, y: H - 6}, svg); lo.textContent = spec.fmt(hist[0][0]);
    lo.setAttribute("fill", css("--text-muted"));
    var hi = el("text", {x: W - m.r, y: H - 6, "text-anchor": "end"}, svg); hi.textContent = spec.fmt(hist[nb - 1][1]);
    hi.setAttribute("fill", css("--text-muted"));
  }

  // ---- failures
  function renderFailures() {
    var box = document.getElementById("failtable"); box.textContent = "";
    if (!D.failures.length) { h("div", "empty", "No failed attempts.", box); return; }
    var table = h("table", null, null, box);
    var hr = h("tr", null, null, h("thead", null, null, table));
    ["When", "Stage", "Task", "Attempt", "Slurm", "Node", "Now", "Reason"].forEach(function (c) { h("th", c === "Attempt" ? "num" : null, c, hr); });
    var body = h("tbody", null, null, table);
    D.failures.forEach(function (f) {
      var tr = h("tr", null, null, body);
      h("td", null, f.t ? fmtTime(f.t) : "–", tr);
      h("td", null, f.stage, tr);
      h("td", null, f.name, tr);
      h("td", "num", f.attempt + " / " + D.max_attempts, tr);
      h("td", null, f.slurm, tr);
      h("td", null, f.node, tr);
      var st = STATE.filter(function (x) { return x.k === f.task_status; })[0] || {label: f.task_status};
      var sc = h("td", null, null, tr), span = h("span", "status", null, sc);
      if (st.color) { var dt = h("span", "sw", null, span); dt.style.cssText = "width:10px;height:10px;border-radius:3px;display:inline-block;background:" + css(st.color); }
      if (st.icon) h("span", null, st.icon, span);
      h("span", null, st.label, span);
      h("td", "reason", (f.sched_state && f.sched_state !== "COMPLETED" ? f.sched_state + ": " : "") + f.reason, tr);
    });
  }

  var REG = EMB.registry || null;
  function renderCampaignNav() {
    var nav = document.getElementById("campnav");
    if (!REG || !REG.length || EMB.to_base === undefined) { nav.hidden = true; return; }
    nav.hidden = false;
    var sel = document.getElementById("campsel");
    sel.textContent = "";
    var list = REG.slice().sort(function (a, b) { return (b.updated || 0) - (a.updated || 0); });
    list.forEach(function (r) {
      var o = document.createElement("option");
      o.value = r.path;
      o.textContent = r.campaign + (r.site ? "  (" + r.site + ")" : "");
      if (r.campaign === D.campaign) o.selected = true;
      sel.appendChild(o);
    });
    document.getElementById("alllink").setAttribute("href", EMB.to_base + "/index.html");
  }
  document.getElementById("campsel").addEventListener("change", function (e) {
    location.href = EMB.to_base + "/" + e.target.value + "/index.html";
  });

  function renderAll() {
    renderCampaignNav();
    renderHeader(); renderKpis(); renderStageBars(); renderCumulative(); renderStats(); renderHists(); renderFailures();
  }
  // theme toggle (remembered per browser)
  var saved = null;
  try { saved = localStorage.getItem("dprod-theme"); } catch (e) {}
  if (saved) document.documentElement.setAttribute("data-theme", saved);
  document.getElementById("themebtn").addEventListener("click", function () {
    var dark = document.documentElement.getAttribute("data-theme") === "dark" ||
      (!document.documentElement.getAttribute("data-theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
    var next = dark ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    try { localStorage.setItem("dprod-theme", next); } catch (e) {}
    renderAll();
  });
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", renderAll);
  var rt; window.addEventListener("resize", function () { clearTimeout(rt); rt = setTimeout(renderAll, 150); });
  renderAll();
  setInterval(renderHeader, 60000);
  // Live update: re-fetch status.json (written next to this page by `dprod web` /
  // `dprod watch --web`) and redraw in place. When opened as a local file, where
  // fetch is not allowed, the page shows the data embedded at generation time.
  function refresh() {
    if (location.protocol === "file:") return;
    var names = {controller: "status.json", jobs: "jobs.json"};
    Promise.all(Object.keys(names).map(function (k) {
      return fetch(names[k] + "?t=" + Date.now(), {cache: "no-store"})
        .then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); })
        .then(function (d) { SRC[k] = d; })
        .catch(function () {});
    }).concat(EMB.to_base === undefined ? [] : [
      fetch(EMB.to_base + "/campaigns.json?t=" + Date.now(), {cache: "no-store"})
        .then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); })
        .then(function (reg) { REG = Object.keys(reg.campaigns || {}).map(function (k) { return reg.campaigns[k]; }); renderCampaignNav(); })
        .catch(function () {})
    ])).then(function () {
      var n = pick();
      if (n !== D) { D = n; renderAll(); } else renderHeader();
    });
  }
  Array.prototype.forEach.call(document.querySelectorAll("#srcctl button"), function (b) {
    b.addEventListener("click", function () {
      choice = b.getAttribute("data-src");
      try { localStorage.setItem("dprod-src", choice); } catch (e) {}
      D = pick(); renderAll();
    });
  });
  refresh();
  setInterval(refresh, Math.max(30, Math.min(D.refresh_s, 120)) * 1000);
})();
</script>
</body>
</html>
"""


# All-campaigns overview (<web base>/index.html). Same stylesheet as the campaign page.
HUB = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DORAEMON production campaigns</title>
<style>__STYLE__
.minibar { display: inline-flex; flex-direction: column; gap: 2px; margin-right: 12px; vertical-align: top; }
td.stages { white-space: nowrap; }
.minibar .lab { font-size: 12px; color: var(--text-secondary); white-space: nowrap; }
td a { color: var(--text-primary); font-weight: 600; }
</style>
</head>
<body>
<div class="viz-root" id="root">
  <header>
    <h1>DORAEMON production campaigns</h1>
    <span class="spacer"></span>
    <span class="badge" id="freshness"></span>
    <button class="theme" id="themebtn" type="button">Theme</button>
  </header>
  <section class="card">
    <h2>Campaigns</h2>
    <p class="desc">Most recently updated first. Stage bars show tasks per stage; select a campaign for details.</p>
    <div class="legend" id="legend"></div>
    <div id="table"></div>
  </section>
  <div id="tip" role="status" aria-live="polite"></div>
</div>
<script type="application/json" id="data">__DATA__</script>
<script>
(function () {
  "use strict";
  var R = JSON.parse(document.getElementById("data").textContent);
  var SVGNS = "http://www.w3.org/2000/svg";
  var STATE = [
    {k: "done", label: "Done", color: "--good", icon: "✓"},
    {k: "running", label: "Running", color: "--st-running"},
    {k: "queued", label: "Queued", color: "--st-queued"},
    {k: "failed", label: "Failed", color: "--critical", icon: "✕"},
    {k: "lost", label: "Lost (no record)", color: "--warning", icon: "?"},
    {k: "rest", label: "Not submitted", color: "--st-new"}
  ];
  function css(v) { return getComputedStyle(document.getElementById("root")).getPropertyValue(v).trim(); }
  function h(tag, cls, text, parent) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    if (parent) parent.appendChild(e);
    return e;
  }
  function el(tag, attrs, parent) {
    var e = document.createElementNS(SVGNS, tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }
  function fmt(n) {
    if (n === null || n === undefined) return "–";
    var a = Math.abs(n);
    if (a >= 1e9) return (n / 1e9).toFixed(1) + "B";
    if (a >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (a >= 1e4) return (n / 1e3).toFixed(1) + "K";
    return Math.round(n).toLocaleString();
  }
  function ago(t) {
    if (!t) return "never";
    var m = (Date.now() / 1000 - t) / 60;
    return m < 1 ? "just now" : m < 120 ? Math.round(m) + " min ago" :
           m < 2880 ? (m / 60).toFixed(1) + " h ago" : Math.round(m / 1440) + " days ago";
  }
  var tip = document.getElementById("tip");
  function showTip(evt, title, rows) {
    tip.textContent = "";
    h("div", "t", title, tip);
    rows.forEach(function (r) {
      var row = h("div", "row", null, tip);
      var k = h("span", "k", null, row); k.style.background = r.color;
      h("b", null, r.value, row); h("span", null, r.label, row);
    });
    tip.style.display = "block";
    var x = evt.clientX + 14, y = evt.clientY + 14;
    if (x + tip.offsetWidth > window.innerWidth - 8) x = evt.clientX - tip.offsetWidth - 14;
    tip.style.left = x + "px"; tip.style.top = y + "px";
  }
  function hideTip() { tip.style.display = "none"; }
  function render() {
    var lg = document.getElementById("legend"); lg.textContent = "";
    STATE.forEach(function (st) {
      var k = h("span", "key", null, lg);
      var sw = h("span", "sw", null, k); sw.style.background = css(st.color);
      if (st.icon) h("span", "ic", st.icon, k);
      h("span", null, st.label, k);
    });
    var camps = Object.keys(R.campaigns || {}).map(function (k) { return R.campaigns[k]; })
      .sort(function (a, b) { return (b.updated || 0) - (a.updated || 0); });
    var box = document.getElementById("table"); box.textContent = "";
    if (!camps.length) { h("div", "empty", "No campaigns yet.", box); return; }
    var table = h("table", null, null, box);
    var hr = h("tr", null, null, h("thead", null, null, table));
    ["Campaign", "Site", "Updated", "Events generated", "Stages", "Failed"].forEach(function (c, i) {
      h("th", i === 3 || i === 5 ? "num" : null, c, hr);
    });
    var body = h("tbody", null, null, table);
    camps.forEach(function (c) {
      var tr = h("tr", null, null, body);
      var td = h("td", null, null, tr);
      var a = h("a", null, c.campaign, td); a.href = c.path + "/index.html";
      if (c.description) h("div", "muted", c.description.slice(0, 90), td);
      h("td", null, c.site, tr);
      h("td", null, ago(c.updated) + (c.source === "jobs" ? " (job records)" : ""), tr);
      var pct = c.planned_events ? Math.round(100 * c.events / c.planned_events) : 0;
      h("td", "num", fmt(c.events) + " / " + fmt(c.planned_events) + " (" + pct + "%)", tr);
      var sc = h("td", "stages", null, tr);
      var failed = 0;
      c.stages.forEach(function (s) {
        failed += s.failed;
        if (!s.enabled) return;
        var mb = h("span", "minibar", null, sc);
        h("span", "lab", s.alias + "  " + fmt(s.done) + "/" + fmt(s.tasks), mb);
        var W = 96, H = 8, svg = el("svg", {width: W, height: H}, mb);
        var cp = el("clipPath", {id: "c" + Math.random().toString(36).slice(2)}, el("defs", {}, svg));
        el("rect", {x: 0, y: 0, width: W, height: H, rx: 3}, cp);
        var g = el("g", {"clip-path": "url(#" + cp.id + ")"}, svg);
        var n = {done: s.done, running: s.running, queued: s.queued, failed: s.failed, lost: s.lost || 0};
        n.rest = Math.max(0, s.tasks - n.done - n.running - n.queued - n.failed - n.lost);
        var x = 0, tot = Math.max(s.tasks, 1);
        STATE.forEach(function (st) {
          if (!n[st.k]) return;
          var w = W * n[st.k] / tot;
          el("rect", {x: x, y: 0, width: Math.max(w - 2, 1), height: H, fill: css(st.color)}, g);
          x += w;
        });
        var hit = el("rect", {x: 0, y: -6, width: W, height: H + 12, fill: "transparent"}, svg);
        hit.addEventListener("pointermove", function (e) {
          showTip(e, c.campaign + " · " + s.alias + " " + s.name, STATE.filter(function (st) { return n[st.k]; })
            .map(function (st) { return {value: fmt(n[st.k]), label: st.label, color: css(st.color)}; }));
        });
        hit.addEventListener("pointerleave", hideTip);
      });
      h("td", "num", fmt(failed), tr);
    });
    var f = document.getElementById("freshness");
    f.textContent = "list updated " + ago(R.written);
  }
  var saved = null;
  try { saved = localStorage.getItem("dprod-theme"); } catch (e) {}
  if (saved) document.documentElement.setAttribute("data-theme", saved);
  document.getElementById("themebtn").addEventListener("click", function () {
    var dark = document.documentElement.getAttribute("data-theme") === "dark" ||
      (!document.documentElement.getAttribute("data-theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
    var next = dark ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    try { localStorage.setItem("dprod-theme", next); } catch (e) {}
    render();
  });
  render();
  if (location.protocol !== "file:") setInterval(function () {
    fetch("campaigns.json?t=" + Date.now(), {cache: "no-store"})
      .then(function (r) { return r.json(); }).then(function (d) { R = d; render(); }).catch(function () {});
  }, 60000);
})();
</script>
</body>
</html>
"""
HUB = HUB.replace("__STYLE__", PAGE.split("<style>", 1)[1].split("</style>", 1)[0])
