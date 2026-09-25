"""Monitoring reports (controller side)."""

import os
import time

from . import db as D
from . import layout as L


def _fmt_s(x):
    if x is None:
        return "-"
    if x < 120:
        return "%.0fs" % x
    if x < 7200:
        return "%.1fm" % (x / 60)
    return "%.2fh" % (x / 3600)


def stage_summary(c, stage):
    s = c.cfg["stages"][stage]
    con = c.con
    counts = {st: 0 for st in D.TASK_STATES}
    for r in con.execute("SELECT status, COUNT(*) n FROM tasks WHERE stage = ? GROUP BY status",
                         (stage,)):
        counts[r["status"]] = r["n"]
    total = sum(counts.values())
    ready = 0
    if s["parent"]:
        for t in D.tasks(con, stage, [D.NEW]):
            if c.parent_state(s, t)[0]:
                ready += 1
    else:
        ready = counts[D.NEW]
    retry = con.execute(
        "SELECT COUNT(*) FROM tasks t WHERE t.stage = ? AND t.status = 'failed' AND"
        " (SELECT COUNT(*) FROM attempts a WHERE a.stage = t.stage AND a.task_id = t.task_id"
        "  AND a.state != 'cancelled' AND COALESCE(a.uncounted, 0) = 0) < ?",
        (stage, c.cfg["max_attempts"])).fetchone()[0]
    queued = con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state = 'submitted'",
                         (stage,)).fetchone()[0]
    running = con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state = 'running'",
                          (stage,)).fetchone()[0]
    ev = con.execute("SELECT COALESCE(SUM(n_events), 0) FROM tasks WHERE stage = ? AND status = 'done'",
                     (stage,)).fetchone()[0]
    size = con.execute("SELECT COALESCE(SUM(size), 0) FROM files WHERE stage = ?",
                       (stage,)).fetchone()[0]
    t = con.execute("SELECT MIN(w), AVG(w), MAX(w), COUNT(w) FROM (SELECT COALESCE(wall_s, elapsed_s) w"
                    " FROM attempts WHERE stage = ? AND state = 'done')", (stage,)).fetchone()
    res = con.execute(
        "SELECT MAX(max_rss_mb), AVG(avg_rss_mb), AVG(gpu_util_pct), AVG(gpu_mem_used_mb),"
        " MAX(gpu_mem_max_mb) FROM attempts WHERE stage = ? AND state = 'done'", (stage,)).fetchone()
    n_failed_attempts = con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state = 'failed'",
                                    (stage,)).fetchone()[0]
    return {"stage": stage, "alias": s["alias"], "total": total, "counts": counts, "ready": ready,
            "retry": retry, "queued": queued, "running": running, "events": ev, "bytes": size,
            "t_min": t[0], "t_mean": t[1], "t_max": t[2], "n_timed": t[3], "max_rss_mb": res[0],
            "avg_rss_mb": res[1], "gpu_util_pct": res[2], "gpu_mem_mb": res[3], "gpu_mem_max_mb": res[4],
            "failed_attempts": n_failed_attempts}


def print_status(c, out=print):
    out("Campaign %s  site %s  (%s)" % (c.tag, c.site["name"], c.dir))
    out("  as of %s; max_attempts=%d" % (time.strftime("%Y-%m-%d %H:%M:%S"), c.cfg["max_attempts"]))
    hdr = ("%-18s %6s %11s %6s %7s %6s %11s %5s %10s %8s %13s %16s  %s" % (
        "stage", "tasks", "new(ready)", "queue", "running", "done", "failed(rtr)", "aband",
        "events", "size", "RAM avg/max", "GPU util/mem", "wall min/mean/max"))
    out(hdr)
    out("-" * len(hdr))
    for stage in c.cfg["stages"]:
        r = stage_summary(c, stage)
        k = r["counts"]
        label = "%s[%s]" % (stage, r["alias"]) if r["alias"] else stage
        if c.cfg["stages"][stage].get("external"):
            label += "*"
        out("%-18s %6d %11s %6d %7d %6d %11s %5d %10d %8s %13s %16s  %s/%s/%s" % (
            label, r["total"], "%d(%d)" % (k[D.NEW], r["ready"]), r["queued"], r["running"],
            k[D.DONE], "%d(%d)" % (k[D.FAILED], r["retry"]), k[D.ABANDONED], r["events"],
            _fmt_bytes(r["bytes"]),
            "%s/%s" % (_fmt_mb(r["avg_rss_mb"]), _fmt_mb(r["max_rss_mb"])),
            "%s/%s" % ("%.0f%%" % r["gpu_util_pct"], _fmt_mb(r["gpu_mem_mb"]))
            if r["gpu_util_pct"] is not None else "-", _fmt_s(r["t_min"]), _fmt_s(r["t_mean"]), _fmt_s(r["t_max"])))
    out("")
    exts = sorted(set(s["external"] for s in c.cfg["stages"].values() if s.get("external")))
    if exts:
        out("*: inherited from campaign %s (read-only here; imported at every sync)" % ", ".join(exts))
    out("new(ready): not yet submitted (of which all inputs are available)")
    out("failed(rtr): latest attempt failed (of which retryable with `dprod recover`)")
    out("RAM avg/max: mean over done jobs of the time-averaged / peak RSS")
    out("GPU util/mem: mean over done jobs of the time-averaged utilization / memory used")


def _fmt_mb(mb):
    return _fmt_bytes(mb * 1024 * 1024) if mb else "-"


def _fmt_bytes(n):
    for unit in ("B", "K", "M", "G", "T", "P"):
        if n < 1024 or unit == "P":
            return ("%.0f%s" if unit == "B" else "%.1f%s") % (n, unit)
        n /= 1024.0


def print_failures(c, stage, out=print, verbose=False):
    rows = D.tasks(c.con, stage, [D.FAILED, D.ABANDONED])
    if not rows:
        out("no failed or abandoned tasks in %s" % stage)
        return
    for t in rows:
        name = L.task_name(stage, t["first_job"], t["last_job"])
        out("%s task %d  %s  status=%s attempts=%d" % (stage, t["task_id"], name, t["status"],
                                                        t["n_attempts"]))
        for a in c.con.execute("SELECT a.*, s.array_job_id FROM attempts a JOIN submissions s"
                               " ON s.id = a.submission_id WHERE a.stage = ? AND a.task_id = ?"
                               " ORDER BY attempt", (stage, t["task_id"])):
            reason = (a["reason"] or "").strip()
            if not verbose:
                reason = reason.splitlines()[0] if reason else ""
            out("    attempt %d  slurm %s_%d  %s  node=%s  %s" % (
                a["attempt"], a["array_job_id"], a["array_index"], a["sched_state"] or a["state"],
                a["node"] or "-", reason))
            logs = os.path.join(c.dir, L.logs_rel_path(stage, name, a["attempt"]))
            if verbose and os.path.exists(logs):
                out("      logs: %s" % logs)
        if t["note"] and t["status"] == D.ABANDONED:
            out("    note: %s" % t["note"])
