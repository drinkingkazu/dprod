"""Monitoring data without a monitoring process (stdlib only; used by workers).

The monitoring page has two data sources with the same format:

  status.json  written by the controller (`dprod web`, `dprod watch --web`) from the
               bookkeeping database, synced with slurm: the live view.
  jobs.json    rebuilt by the jobs themselves when they finish, from small
               per-attempt record files: the view as of the last finished job.
               Needs no monitoring process, so it keeps working when `watch`
               is not running.

Files (under the campaign directory):
  records/<stage>/<task>_aNN.json   one per attempt, written by the worker at start
                                    (state "running") and again at the end
                                    (state "done" or "failed", with statistics)
  records/plan.json                 written by the controller after every command
                                    that changes bookkeeping: stages, task counts,
                                    abandoned tasks, planned events
  <web dir>/jobs.json, index.html   rebuilt by finishing jobs (rate limited)

What the job-side view cannot know (slurm's side): a job that never started is
"queued" and one killed without writing a record stays "running" until it has
exceeded its time limit, after which it is shown as "lost".
"""

import errno
import fcntl
import glob
import json
import os
import time

REC_DIR = "records"
STATES = ("done", "running", "submitted", "failed", "lost", "abandoned", "new")


def _percentile(sorted_vals, q):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _histogram(vals, n_bins=12):
    if not vals:
        return []
    lo, hi = min(vals), max(vals)
    if hi <= lo:
        return [[lo, hi, len(vals)]]
    w = (hi - lo) / n_bins
    counts = [0] * n_bins
    for v in vals:
        counts[min(int((v - lo) / w), n_bins - 1)] += 1
    return [[lo + i * w, lo + (i + 1) * w, c] for i, c in enumerate(counts)]


def _downsample(points, n=400):
    if len(points) <= n:
        return points
    step = len(points) / float(n)
    out = [points[int(i * step)] for i in range(n)]
    if out[-1] != points[-1]:
        out.append(points[-1])
    return out


def stage_stats(done):
    """Statistics over successful attempts: dicts with t (end), wall, events,
    max_rss, avg_rss, gpu_util, gpu_mem."""
    walls = sorted(r["wall"] for r in done if r.get("wall") is not None)
    cum, total = [], 0
    for r in sorted((r for r in done if r.get("t")), key=lambda r: r["t"]):
        total += r.get("events") or 0
        cum.append([round(r["t"], 1), total])

    def avg(key):
        vals = [r[key] for r in done if r.get(key) is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        "events": sum(r.get("events") or 0 for r in done),
        "wall": {"min": walls[0] if walls else None, "p50": _percentile(walls, 0.5),
                 "mean": sum(walls) / len(walls) if walls else None,
                 "max": walls[-1] if walls else None, "n": len(walls)},
        "wall_hist": _histogram(walls),
        "ram_avg": avg("avg_rss"),
        "ram_max": max((r["max_rss"] for r in done if r.get("max_rss")), default=None),
        "gpu_util": avg("gpu_util"), "gpu_mem": avg("gpu_mem"),
        "cumulative": _downsample(cum),
    }


def write_json_atomic(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"), default=str)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- records (worker)

def record_path(campaign_dir, stage, name, attempt):
    return os.path.join(campaign_dir, REC_DIR, stage, "%s_a%02d.json" % (name, attempt))


def write_record(campaign_dir, stage, task, state, summary=None, time_limit_s=None):
    """Worker: the attempt's record (small; the full summary stays in summaries/)."""
    s = summary or {}
    res = s.get("resources") or {}
    rec = {"stage": stage, "task_id": task["task_id"], "attempt": task["attempt"],
           "name": task["name"], "first_job": task["first_job"], "last_job": task["last_job"],
           "state": state, "host": s.get("host") or os.uname().nodename,
           "slurm": "%s_%s" % (os.environ.get("SLURM_ARRAY_JOB_ID", "?"),
                               os.environ.get("SLURM_ARRAY_TASK_ID", "?")),
           "start": s.get("start") or time.time(), "end": s.get("end"),
           "wall": s.get("wall_s"), "events": s.get("n_events"),
           "bytes": sum(f.get("size") or 0 for f in s.get("files") or []),
           "max_rss": s.get("max_rss_mb"), "avg_rss": res.get("avg_rss_mb"),
           "gpu_util": res.get("gpu_util_pct"), "gpu_mem": res.get("gpu_mem_used_mb"),
           "time_limit_s": time_limit_s,
           "reason": (s.get("reason") or "").strip().splitlines()[0][:300] if s.get("reason") else ""}
    write_json_atomic(record_path(campaign_dir, stage, task["name"], task["attempt"]), rec)


# --------------------------------------------------------------------------- plan (controller)

def write_plan(c):
    """Controller: what the job-side view needs to know about the campaign."""
    con = c.con
    stages = []
    for name, s in c.cfg["stages"].items():
        n = con.execute("SELECT COUNT(*) FROM tasks WHERE stage = ?", (name,)).fetchone()[0]
        ab = [r[0] for r in con.execute(
            "SELECT task_id FROM tasks WHERE stage = ? AND status = 'abandoned'", (name,))]
        cx = [[r[0], r[1]] for r in con.execute(
            "SELECT task_id, attempt FROM attempts WHERE stage = ? AND state = 'cancelled'", (name,))]
        stages.append({"name": name, "alias": s["alias"] or name, "parent": s["parent"],
                       "enabled": s["enabled"], "merge": s["merge"], "tasks": n, "abandoned": ab,
                       "cancelled": cx})
    root = c.cfg["root_stage"]
    n_root = stages[0]["tasks"]
    web = c.site.get("web") or {}
    plan = {"campaign": c.tag, "site": c.site["name"], "description": c.cfg.get("description", ""),
            "max_attempts": c.cfg["max_attempts"], "planned_jobs": n_root,
            "planned_events": n_root * int(c.cfg["stages"][root]["events_per_job"] or 0),
            "refresh_s": int(web.get("refresh_s", 600)), "stages": stages,
            "written": time.time()}
    write_json_atomic(os.path.join(c.dir, REC_DIR, "plan.json"), plan)
    return plan


# --------------------------------------------------------------------------- collection (anyone)

def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def from_records(campaign_dir, now=None):
    """The page data (same format as the controller's) from plan.json + records."""
    now = now or time.time()
    plan = _load(os.path.join(campaign_dir, REC_DIR, "plan.json"))
    if plan is None:
        raise FileNotFoundError("no %s/plan.json (run any dprod command once)" % REC_DIR)
    # submitted-but-not-started tasks come from the submission manifests
    cancelled = set((ps["name"], tid, att) for ps in plan["stages"]
                    for tid, att in ps.get("cancelled") or [])
    submitted = {}
    for m in glob.glob(os.path.join(campaign_dir, "submissions", "*", "sub_*.json")):
        d = _load(m)
        if d:
            for t in d.get("tasks", []):
                key = (d["stage"], t["task_id"])
                if (d["stage"], t["task_id"], t["attempt"]) not in cancelled:
                    submitted[key] = max(submitted.get(key, 0), t["attempt"])
    latest, attempts = {}, {}
    for p in glob.glob(os.path.join(campaign_dir, REC_DIR, "*", "*_a*.json")):
        r = _load(p)
        if not r or "stage" not in r or (r["stage"], r["task_id"], r["attempt"]) in cancelled:
            continue
        key = (r["stage"], r["task_id"])
        attempts.setdefault(r["stage"], []).append(r)
        if key not in latest or r["attempt"] > latest[key]["attempt"]:
            latest[key] = r
    stages, active, failures = [], {}, []
    for ps in plan["stages"]:
        name = ps["name"]
        counts = {k: 0 for k in STATES}
        abandoned = set(ps.get("abandoned") or [])
        queued = running = 0
        seen = set()
        for (st, tid), r in latest.items():
            if st != name:
                continue
            seen.add(tid)
            if tid in abandoned:
                counts["abandoned"] += 1
            elif submitted.get((st, tid), 0) > r["attempt"]:
                counts["submitted"] += 1          # a newer attempt is queued
                queued += 1
            elif r["state"] == "running":
                lim = r.get("time_limit_s")
                if lim and now - r["start"] > lim + 600:
                    counts["lost"] += 1
                else:
                    counts["running"] += 1
                    running += 1
            else:
                counts[r["state"] if r["state"] in counts else "failed"] += 1
        for (st, tid), att in submitted.items():
            if st == name and tid not in seen:
                if tid in abandoned:
                    counts["abandoned"] += 1
                else:
                    counts["submitted"] += 1
                    queued += 1
                seen.add(tid)
        counts["new"] = max(0, ps["tasks"] - sum(counts.values()))
        done = [r for (st, _), r in latest.items() if st == name and r["state"] == "done"]
        stats = stage_stats([{"t": r.get("end"), "wall": r.get("wall"), "events": r.get("events"),
                              "max_rss": r.get("max_rss"), "avg_rss": r.get("avg_rss"),
                              "gpu_util": r.get("gpu_util"), "gpu_mem": r.get("gpu_mem")}
                             for r in done])
        att = attempts.get(name, [])
        stage = {"name": name, "alias": ps["alias"], "parent": ps["parent"],
                 "enabled": ps["enabled"], "merge": ps["merge"],
                 "tasks": ps["tasks"], "counts": counts,
                 "planned_events": plan["planned_events"],
                 "bytes": sum(r.get("bytes") or 0 for r in done),
                 "attempts": len(att), "failed_attempts": sum(1 for r in att if r["state"] == "failed")}
        stage.update(stats)
        stages.append(stage)
        active[name] = {"queued": queued, "running": running}
        for r in att:
            if r["state"] == "failed":
                lr = latest[(name, r["task_id"])]
                failures.append({
                    "t": r.get("end"), "stage": name, "task_id": r["task_id"], "name": r["name"],
                    "attempt": r["attempt"], "slurm": r.get("slurm", ""), "node": r.get("host", ""),
                    "sched_state": "",
                    "task_status": "abandoned" if r["task_id"] in abandoned else lr["state"],
                    "reason": r.get("reason", "")})
    failures.sort(key=lambda f: f["t"] or 0, reverse=True)
    last = max([r.get("end") or r.get("start") or 0 for r in latest.values()] or [0])
    return {
        "source": "jobs", "campaign": plan["campaign"], "site": plan["site"],
        "description": plan.get("description", ""), "generated": now,
        "last_record": last or None, "plan_written": plan.get("written"),
        "refresh_s": plan.get("refresh_s", 600), "max_attempts": plan["max_attempts"],
        "planned_jobs": plan["planned_jobs"], "planned_events": plan["planned_events"],
        "active": active, "stages": stages, "failures": failures[:30],
    }


# --------------------------------------------------------------------------- job-side rebuild

def _newer(pattern, t):
    out = []
    for p in glob.glob(pattern):
        try:
            if os.path.getmtime(p) > t:
                out.append(p)
        except OSError:
            pass
    return out


def _still_active(campaign_dir, snapshot):
    """Estimate how many tasks are still queued/running, cheaply: the snapshot's
    active count + tasks submitted since - attempts finished since (only files
    newer than the snapshot are read)."""
    t = os.path.getmtime(snapshot)
    prev = _load(snapshot) or {}
    n = sum(a["queued"] + a["running"] for a in (prev.get("active") or {}).values())
    for m in _newer(os.path.join(campaign_dir, "submissions", "*", "sub_*.json"), t):
        n += len((_load(m) or {}).get("tasks") or [])
    for r in _newer(os.path.join(campaign_dir, REC_DIR, "*", "*_a*.json"), t):
        if ((_load(r) or {}).get("state")) in ("done", "failed"):
            n -= 1
    return n


def rebuild_from_job(campaign_dir, web_dir, min_interval=300, force=False, log=None):
    """Worker, at the end of a task: refresh <web_dir>/jobs.json (+ index.html).

    Rate limited: skipped if jobs.json is younger than min_interval s, unless no
    other task is estimated to be still active (so the last job to finish always
    updates it) or force. Only one job rebuilds at a time (POSIX lock); the others
    skip. Returns True if it rebuilt.
    """
    os.makedirs(web_dir, exist_ok=True)
    out = os.path.join(web_dir, "jobs.json")
    if not force and os.path.exists(out) and time.time() - os.path.getmtime(out) < min_interval:
        if _still_active(campaign_dir, out) > 0:
            return False
    lock = open(os.path.join(web_dir, ".jobs.lock"), "a")
    try:
        try:
            fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)     # cross-host on parallel FS
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                return False              # another job is rebuilding right now
        data = from_records(campaign_dir)
        from .web import write_files   # page template lives with the controller code
        write_files(web_dir, data, "jobs.json")
        if log:
            log("monitoring snapshot updated: %s" % out)
        return True
    finally:
        lock.close()
