"""dprod: DORAEMON production controller.

Run on a login node (needs sbatch/sacct). The campaign is selected with
--site/--campaign or the DPROD_SITE/DPROD_CAMPAIGN environment variables.

  dprod init                          # asks: site, campaign config; becomes the current campaign
  dprod sites | configs | campaigns   # what exists; `dprod use <tag>` switches the current campaign
  dprod submit 1 --limit 200          # stage 1 (alias) or edepsim (name)
  dprod status                        # sync + per-stage summary
  dprod submit 2A                     # downstream: submits whatever inputs are ready
  dprod advance --recover             # all stages at once: everything ready (+ retries)
  dprod watch --recover               # repeat advance every 10 min until done (use tmux)
  dprod failures 1                    # why did tasks fail?
  dprod recover 1                     # resubmit failed tasks (< max_attempts)
  dprod mark 1 17,20-25 --abandon     # give up on tasks; downstream proceeds without
  dprod move 2A --partition turing --account X   # re-route queued jobs in place
  dprod cancel 1 --queued; dprod submit 1 --partition roma   # or cancel + resubmit
  dprod lookup 123 45                 # files holding job 123 event 45
  dprod merge-summary                 # per-stage summary HDF5 (job/event/particle tables)
"""

import argparse
import os
import shlex
import subprocess
import sys
import time

from . import config as C
from . import db as D
from . import layout as L
from .campaign import Campaign, CampaignError, _span
from .report import print_failures, print_status
from .scheduler import SchedulerError


def parse_ids(text):
    """'3,5-7,10' -> [3,5,6,7,10]"""
    if text is None:
        return None
    out = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


# commands that only read bookkeeping; everything else holds the campaign lock
READ_ONLY = ("lookup", "files", "tasks", "watch", "sites", "configs", "campaigns", "use")


def _slurm_override(a):
    """--partition/--account/--qos/--time/--slurm KEY=VALUE -> dict of slurm options."""
    o = {}
    for k in ("partition", "account", "qos", "time"):
        v = getattr(a, k, None)
        if v:
            o[k] = v
    for kv in getattr(a, "slurm", None) or []:
        if "=" not in kv:
            raise CampaignError("--slurm expects KEY=VALUE, got %r" % kv)
        k, v = kv.split("=", 1)
        o[k.strip().lstrip("-").replace("-", "_")] = v
    return o


def _add_slurm_args(p, what="submissions"):
    g = p.add_argument_group("slurm overrides (for these %s only; site config unchanged)" % what)
    g.add_argument("--partition")
    g.add_argument("--account")
    g.add_argument("--qos")
    g.add_argument("--time", help="e.g. 04:00:00")
    g.add_argument("--slurm", action="append", metavar="KEY=VALUE",
                   help="any other sbatch option, e.g. --slurm mem=64G --slurm constraint=a100")


def _note(msg):
    print("dprod: %s" % msg, file=sys.stderr)


def _open(a):
    from . import select as SEL
    try:
        site_name = SEL.pick_site(a.site, note=_note)
        site = C.load_site(site_name)
        chosen = not a.campaign and not (SEL.load_state().get("campaign") and
                                         SEL.load_state().get("site") == site["name"])
        tag = SEL.pick_campaign(site, a.campaign, note=_note)
    except SEL.SelectError as e:
        raise CampaignError(str(e))
    if chosen:                       # picked from the menu: remember it
        SEL.save_state(site_name, tag)
    c = Campaign(site, tag)
    if a.cmd not in READ_ONLY:
        # held until the command returns (released in main)
        cm = c.lock()
        cm.__enter__()
        _HELD_LOCKS.append((c, cm))
    return c


_HELD_LOCKS = []


def _stages(c, arg):
    if not arg or arg == "all":
        return list(c.cfg["stages"])
    return [C.resolve_stage(c.cfg, x) for x in arg.split(",")]


def cmd_init(a):
    from . import select as SEL
    try:
        site_name = SEL.pick_site(a.site, note=_note)
        site = C.load_site(site_name)
        cfg_path = a.config
        if not cfg_path:
            configs = SEL.list_configs()
            existing = set(c["tag"] for c in SEL.list_campaigns(site))
            opts = [(p, "%-42s campaign: %s%s" % (os.path.relpath(p, C.REPO_DIR), tag,
                                                 "   (already exists)" if tag in existing else ""))
                    for p, tag, _ in configs]
            cfg_path = SEL.choose("Campaign config:", opts, what="config file")
    except SEL.SelectError as e:
        raise CampaignError(str(e))
    tag = (C.load_yaml(cfg_path) or {}).get("campaign")
    target = os.path.join(site["storage_root"], str(tag))
    print("campaign tag (from the config): %s" % tag)
    print("site %s -> %s" % (site["name"], target))
    if os.path.exists(os.path.join(target, "campaign.yaml")):
        raise CampaignError("campaign %s already exists at %s; change `campaign:` in %s for a new one "
                            "(or `dprod destroy` the old one)" % (tag, target, cfg_path))
    if not a.config and not _batch(a):
        if input("Initialize this campaign? [y/N] ").strip().lower() not in ("y", "yes"):
            print("not initialized")
            return
    c = Campaign.init(site, cfg_path)
    SEL.save_state(site_name, c.tag)
    print("initialized campaign %s at %s" % (c.tag, c.dir))
    print("  database: %s" % c.db_path)
    for stage in c.cfg["stages"]:
        n = c.con.execute("SELECT COUNT(*) FROM tasks WHERE stage = ?", (stage,)).fetchone()[0]
        print("  %-14s %d task(s) defined" % (stage, n))
    print("this is now your current campaign; next: dprod submit %s" % (
        c.cfg["stages"][c.cfg["root_stage"]]["alias"] or c.cfg["root_stage"]))


def cmd_sites(a):
    from . import select as SEL
    cur = SEL.load_state().get("site")
    for name, path, site, usable in SEL.list_sites():
        print("%s %-8s %-45s %s" % ("*" if name == cur else " ", name,
                                    site["storage_root"] if site else "(invalid config)",
                                    "usable on this machine" if usable else ""))
    print("(* = current; configs in %s)" % os.path.relpath(C.SITE_DIR, os.getcwd()))


def cmd_configs(a):
    from . import select as SEL
    for p, tag, desc in SEL.list_configs():
        print("%-45s campaign: %-32s %s" % (os.path.relpath(p, os.getcwd()), tag, desc[:60]))


def cmd_campaigns(a):
    from . import select as SEL
    try:
        site = C.load_site(SEL.pick_site(a.site, note=_note))
    except SEL.SelectError as e:
        raise CampaignError(str(e))
    st = SEL.load_state()
    camps = SEL.list_campaigns(site)
    if not camps:
        print("no campaigns at site %s (%s)" % (site["name"], site["storage_root"]))
    for c in camps:
        cur = st.get("campaign") == c["tag"] and st.get("site") == site["name"]
        print("%s %-34s %s" % ("*" if cur else " ", c["tag"], SEL.progress_text(c)))
    if camps:
        print("(* = current; switch with `dprod use <campaign>`)")


def cmd_use(a):
    from . import select as SEL
    try:
        site_name = a.site or SEL.pick_site(None, note=_note)
        site = C.load_site(site_name)
        if a.tag:
            tag = a.tag
        else:
            camps = SEL.list_campaigns(site)
            cur = SEL.load_state().get("campaign")
            tag = SEL.choose("Campaign at site %s:" % site["name"],
                             [(c["tag"], "%-34s %s" % (c["tag"], SEL.progress_text(c))) for c in camps],
                             default=cur if cur in [c["tag"] for c in camps] else None,
                             what="campaign")
    except SEL.SelectError as e:
        raise CampaignError(str(e))
    if not os.path.exists(os.path.join(site["storage_root"], tag, "campaign.yaml")):
        raise CampaignError("no campaign %s at site %s (see `dprod campaigns`)" % (tag, site["name"]))
    SEL.save_state(site_name, tag)
    print("current campaign: %s (site %s)" % (tag, site["name"]))


def cmd_extend(a):
    c = _open(a)
    n = c.extend(a.n_jobs)
    print("added %d job(s); campaign now has %d" % (n, a.n_jobs))


def print_plan(c, items, out=print):
    rows = c.plan_summary(items)
    out("Submission plan for campaign %s (site %s):" % (c.tag, c.site["name"]))
    hdr = "  %-5s %-5s %6s %6s %13s %9s  %-10s %-18s %-12s %-9s %s" % (
        "stage", "kind", "tasks", "arrays", "stage-1 jobs", "events", "partition", "account", "qos",
        "time", "gpus")
    out(hdr)
    for r in rows:
        out("  %-5s %-5s %6d %6d %13s %9s  %-10s %-18s %-12s %-9s %s" % (
            r["alias"], r["kind"], r["tasks"], r["arrays"],
            "%d (%s)" % (r["jobs"], r["job_range"]) if r["jobs"] > 1 else r["job_range"],
            "{:,}".format(r["events"]), r["partition"] or "-", r["account"] or "-", r["qos"] or "-",
            r["time"] or "-", r["gpus"] or "-"))
    out("  total: %d task(s) in %d array(s), %s events" % (
        sum(r["tasks"] for r in rows), sum(r["arrays"] for r in rows),
        "{:,}".format(sum(r["events"] for r in rows))))
    if c.slurm_override:
        out("  slurm overrides (this command only): %s" % " ".join(
            "%s=%s" % kv for kv in c.slurm_override.items()))
    return rows


def _batch(a):
    return getattr(a, "batch", False) or os.environ.get("DPROD_BATCH", "") not in ("", "0")


def confirm_plan(c, items, a):
    """Show the plan; ask yes/no unless --batch / DPROD_BATCH / --dry-run. True = go."""
    if not items:
        return False
    print_plan(c, items)
    if getattr(a, "dry_run", False) or _batch(a):
        return True
    if not sys.stdin.isatty():
        raise CampaignError("not a terminal: add --batch (or set DPROD_BATCH=1) to submit without "
                            "confirmation")
    answer = input("Proceed? [y/N] ").strip().lower()
    if answer not in ("y", "yes"):
        print("not submitted")
        return False
    return True


def _do_submit(a, recovery):
    c = _open(a)
    c.slurm_override = _slurm_override(a)
    c.sync()
    stage = C.resolve_stage(c.cfg, a.stage)
    entries = c.plan(stage, recovery=recovery, task_ids=parse_ids(a.tasks), limit=a.limit,
                     force=getattr(a, "force", False), reseed=getattr(a, "reseed", False))
    if entries and not confirm_plan(c, [(stage, recovery, entries)], a):
        return
    res = c.submit(stage, recovery=recovery, dry_run=a.dry_run, entries=entries) if entries else []
    if not res:
        what = "failed tasks eligible for recovery" if recovery else "ready new tasks"
        print("%s: no %s" % (stage, what))
        return
    for r in res:
        ids = r["task_ids"]
        span = "%d-%d" % (ids[0], ids[-1]) if ids == list(range(ids[0], ids[-1] + 1)) else \
            ",".join(map(str, ids[:10])) + (",..." if len(ids) > 10 else "")
        if r.get("dry_run"):
            print("[dry-run] %s: %d task(s) [%s]; script %s" % (stage, r["n_tasks"], span, r["script"]))
        else:
            print("%s: submitted %d task(s) [%s] as array %s (submission %d)" % (
                stage, r["n_tasks"], span, r["array_job_id"], r["submission_id"]))


def cmd_submit(a):
    _do_submit(a, recovery=False)


def cmd_recover(a):
    _do_submit(a, recovery=True)


def cmd_advance(a):
    c = _open(a)
    c.slurm_override = _slurm_override(a)
    stages = _stages(c, a.stages) if a.stages else None
    plan = c.plan_advance(stages, recover=a.recover, max_queued=a.max_queued)
    if not plan:
        print("nothing ready to submit")
    elif confirm_plan(c, plan, a):
        c.advance(recover=a.recover, dry_run=a.dry_run, plan=plan)
    p = c.progress()
    print("active %d, ready %d, retryable failed %d" % (p["active"], p["ready"], p["retryable"]))
    p = c.progress()
    print("active %d, ready %d, retryable failed %d" % (p["active"], p["ready"], p["retryable"]))


def cmd_watch(a):
    """Loop: advance, print status, sleep. Stops when nothing can progress."""
    c = _open(a)
    c.slurm_override = _slurm_override(a)
    if c.slurm_override:
        print("slurm overrides for all submissions: %s" % " ".join(
            "%s=%s" % kv for kv in c.slurm_override.items()))
    stages = _stages(c, a.stages) if a.stages else None
    print("dprod watch: campaign %s, every %d s%s%s (Ctrl-C to stop)" % (
        c.tag, a.interval, ", with recovery" if a.recover else "",
        ", max_queued %d" % a.max_queued if a.max_queued is not None else ""))
    rounds = 0
    try:
        while True:
            rounds += 1
            print("\n=== %s round %d" % (time.strftime("%Y-%m-%d %H:%M:%S"), rounds))
            with c.lock():
                plan = c.plan_advance(stages, recover=a.recover, max_queued=a.max_queued)
                if plan:
                    print_plan(c, plan)          # unattended: logged, not asked
                res = c.advance(recover=a.recover, plan=plan)
                if not any(res.values()):
                    print("nothing new to submit")
                c.sync()
                if a.merge_summary:
                    try:
                        c.merge_summary(out=lambda msg: None)
                    except CampaignError as e:
                        print("warning: %s" % e)
                print_status(c)
                p = c.progress()
                from .webdata import write_plan
                write_plan(c)
                if a.web:
                    from .web import write
                    try:
                        print("monitoring page: %s" % write(c, publish=True))
                    except Exception as e:     # never let the page stop production
                        print("warning: monitoring page not written: %s" % e)
            if p["active"] == 0 and p["ready"] == 0 and not any(res.values()) and \
                    not (a.recover and p["retryable"]):
                print("\nnothing queued, running or ready: stopping. Remaining failed or blocked"
                      " tasks need attention (dprod failures; dprod recover / mark).")
                return 0
            if a.once:
                return 0
            time.sleep(a.interval)
    except KeyboardInterrupt:
        print("\nstopped (jobs already submitted keep running; restart watch any time)")
        return 0


def cmd_web(a):
    from .web import write
    c = _open(a)
    if not a.no_sync:
        c.sync()
    path = write(c, out=a.out, publish=a.publish)
    print("wrote %s (and status.json)" % path)


def cmd_sync(a):
    c = _open(a)
    counts = c.sync()
    print("synced %d active attempt(s): %s" % (
        sum(counts.values()), ", ".join("%s=%d" % kv for kv in sorted(counts.items())) or "-"))


def cmd_status(a):
    c = _open(a)
    if not a.no_sync:
        c.sync()
    print_status(c)


def cmd_failures(a):
    c = _open(a)
    if not a.no_sync:
        c.sync()
    for stage in _stages(c, a.stage):
        print_failures(c, stage, verbose=a.verbose)


def cmd_mark(a):
    c = _open(a)
    stage = C.resolve_stage(c.cfg, a.stage)
    status = D.ABANDONED if a.abandon else D.FAILED if a.failed else D.NEW
    n = c.mark(stage, parse_ids(a.tasks), status, note=a.note, force=a.force)
    print("%s: marked %d task(s) %s" % (stage, n, status))


def cmd_cancel(a):
    c = _open(a)
    stage = C.resolve_stage(c.cfg, a.stage) if a.stage else None
    rows = c.cancel(stage, parse_ids(a.tasks), queued_only=a.queued)
    by = {}
    for r in rows:
        by.setdefault(r["stage"], []).append(r["task_id"])
    if not rows:
        print("nothing to cancel")
    for st, ids in by.items():
        print("%s: cancelled %d %s element(s); tasks [%s] are back to 'new' (the attempt does not count)" % (
            st, len(ids), "queued" if a.queued else "queued/running", _span(sorted(ids))))
    if rows:
        print("resubmit with e.g.: dprod submit <stage> --partition P --account A   (or dprod advance ...)")


def cmd_reset_attempts(a):
    c = _open(a)
    c.sync()
    stage = C.resolve_stage(c.cfg, a.stage)
    ids = c.reset_attempts(stage, parse_ids(a.tasks))
    if not ids:
        print("%s: no failed tasks%s" % (stage, " among the given ones" if a.tasks else ""))
        return
    print("%s: retry count reset for %d failed task(s) [%s]; `dprod recover %s` resubmits them" % (
        stage, len(ids), _span(ids), a.stage))


def cmd_move(a):
    c = _open(a)
    stage = C.resolve_stage(c.cfg, a.stage) if a.stage else None
    changes = _slurm_override(a)
    if not changes:
        raise CampaignError("give at least one of --partition/--account/--qos/--time")
    rows = c.move(stage, changes, parse_ids(a.tasks))
    print("updated %d queued element(s): %s" % (len(rows), " ".join("%s=%s" % kv for kv in changes.items())))
    if rows:
        print("check with: squeue -u $USER -o '%.18i %.9P %.12a %.8T %.10l'")


def cmd_lookup(a):
    c = _open(a)
    stage = C.resolve_stage(c.cfg, a.stage) if a.stage else None
    rows = D.lookup_event(c.con, a.job, a.event, stage)
    if not rows:
        print("job %d event %d: not found%s" % (a.job, a.event, " in " + stage if stage else ""))
        return 1
    for r in rows:
        print("%-14s %-10s task %-6d %s" % (r["stage"], r["role"], r["task_id"],
                                            os.path.join(c.dir, r["path"])))


def cmd_files(a):
    c = _open(a)
    stage = C.resolve_stage(c.cfg, a.stage)
    q = "SELECT * FROM files WHERE stage = ?"
    args = [stage]
    if a.role:
        q += " AND role = ?"
        args.append(a.role)
    for r in c.con.execute(q + " ORDER BY first_job, role, path", args):
        print(os.path.join(c.dir, r["path"]) if not a.long else "%s\t%s\t%s\t%s\t%s" % (
            r["first_job"], r["last_job"], r["n_events"], r["role"], os.path.join(c.dir, r["path"])))


def cmd_tasks(a):
    c = _open(a)
    stage = C.resolve_stage(c.cfg, a.stage)
    statuses = a.status.split(",") if a.status else None
    def f(v, fmt):
        return fmt % v if v is not None else "-"
    print("%6s  %-34s %-10s %4s %6s %8s %8s %8s %6s %8s  %s" % (
        "task", "name", "status", "att", "events", "wall_s", "RAMavgMB", "RAMmaxMB",
        "GPU%", "GPUmemMB", "note"))
    for t in D.tasks(c.con, stage, statuses, parse_ids(a.tasks)):
        at = c.con.execute("SELECT * FROM attempts WHERE stage = ? AND task_id = ? AND attempt = ?",
                           (stage, t["task_id"], t["n_attempts"])).fetchone()
        g = (lambda k: at[k] if at is not None else None)
        print("%6d  %-34s %-10s %4d %6s %8s %8s %8s %6s %8s  %s" % (
            t["task_id"], L.task_name(stage, t["first_job"], t["last_job"]), t["status"],
            t["n_attempts"], f(t["n_events"], "%d"), f(g("wall_s"), "%.0f"),
            f(g("avg_rss_mb"), "%.0f"), f(g("max_rss_mb"), "%.0f"), f(g("gpu_util_pct"), "%.0f"),
            f(g("gpu_mem_used_mb"), "%.0f"),
            (t["note"] or "").splitlines()[0][:60] if t["note"] else ""))


def cmd_merge_summary(a):
    c = _open(a)
    c.sync()
    stages = _stages(c, a.stage) if a.stage else None
    for stage, miss in c.merge_summary(stages, rebuild=a.rebuild).items():
        if miss:
            print("warning: %s: %d done task(s) have no summary file (e.g. task %d)" % (
                stage, len(miss), miss[0]))


_IMAGE_CHECKED = {}


def _check_image(site, s, report, variables=None):
    """Start the stage's container here and import what the worker and the stage
    need (catches unreadable images, missing python3/h5py/jax/pysupera...), with the
    stage's `pythonpath` first, as in the job."""
    import subprocess
    prefix = C.container_prefix(site, dict(s, container_flags=""), login=True)
    mods = ["doraemon_prod.worker"] + list(s.get("check_imports") or [])
    code = ("import importlib, os, sys; ms = [importlib.import_module(m) for m in %r]; "
            "print('python ' + sys.version.split()[0] + ''.join('; %%s from %%s' %% (m.__name__, "
            "os.path.dirname(os.path.dirname(m.__file__))) for m in ms[2:] if getattr(m, '__file__', None)))"
            % mods)
    pp = []
    for p in s.get("pythonpath") or []:
        try:
            pp.append(str(p).format(**(variables or {})))
        except KeyError as e:
            report(False, "pythonpath %s" % p, "unknown variable %s" % e)
    # the environment the job's stage command gets (site `env`, stage `env`), so that
    # e.g. PYTHONNOUSERSITE or a PATH change affects the check as it affects the job
    env = C.stage_env(site, s)
    exports = "".join('export %s="%s"; ' % (k, str(v).replace('"', '\\"')) for k, v in sorted(env.items()))
    exports += 'export PYTHONPATH=%s${PYTHONPATH:+:$PYTHONPATH}; ' % shlex.quote(":".join(pp + [C.REPO_DIR]))
    key = (prefix, tuple(mods), tuple(pp), tuple(sorted(env.items())))
    if key not in _IMAGE_CHECKED:
        cmd = "%s bash -c %s" % (prefix, shlex.quote(exports + "python3 -c " + shlex.quote(code)))
        try:
            p = subprocess.run(cmd.strip(), shell=True, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, universal_newlines=True, timeout=600)
            out = [ln for ln in p.stdout.splitlines() if ln.strip()]
            _IMAGE_CHECKED[key] = (p.returncode == 0, out[-1] if out else "")
        except subprocess.TimeoutExpired:
            _IMAGE_CHECKED[key] = (False, "timed out after 600 s")
    good, last = _IMAGE_CHECKED[key]
    what = "container starts, imports %s" % ", ".join(mods[1:] or ["worker"])
    if env:
        what += " (with %s)" % ", ".join("%s=%s" % (k, v if len(str(v)) < 20 else "...")
                                         for k, v in sorted(env.items()))
    report(good, what, last if good else last[:300])


def cmd_check(a):
    """Preflight checks for a site (+ campaign config) before the first submission."""
    import re
    import shutil
    ok = [True]

    def report(good, what, detail=""):
        print("  [%s] %s%s" % ("ok" if good else "FAIL", what, ("  " + detail) if detail else ""))
        ok[0] &= bool(good)

    if not a.site:
        raise CampaignError("specify --site")
    site = C.load_site(a.site)
    print("site %s (%s)" % (site["name"], site["_path"]))
    report(sys.version_info >= (3, 6), "python >= 3.6", sys.version.split()[0])
    tools = ["sbatch", "sacct", "scancel"] if site["scheduler"] == "slurm" else []
    exe = (site["container"].get("exec") or "").split()
    if exe and exe[0] != "{image}":
        tools.append(exe[0])
    for t in tools:
        report(shutil.which(t), "command %s" % t, shutil.which(t) or "not on PATH")
    for key in ("storage_root", "log_root"):
        d = site[key]
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".dprod_write_test")
            open(probe, "w").close()
            os.remove(probe)
            report(True, "%s writable" % key, d)
        except OSError as e:
            report(False, "%s writable" % key, "%s: %s" % (d, e))
    if not a.config:
        print("(pass a campaign config to also check images and software paths)")
        return 0 if ok[0] else 1

    cfg = C.load_campaign(a.config, site)
    cdir = os.path.join(site["storage_root"], cfg["campaign"])
    report(not os.path.exists(os.path.join(cdir, "campaign.yaml")),
           "campaign tag %s unused" % cfg["campaign"],
           "" if not os.path.exists(cdir) else "exists: %s" % cdir)
    fmt = dict(site["vars"])
    for name, s in cfg["stages"].items():
        print("stage %s%s%s" % (name, " [%s]" % s["alias"] if s["alias"] else "",
                                "" if s["enabled"] else "  (disabled)"))
        try:
            opts = C.slurm_options(site, s)
            report(True, "slurm options", " ".join("%s=%s" % kv for kv in opts.items()
                                                   if kv[1] not in (None, "", False)))
            C.container_prefix(site, s)
        except C.ConfigError as e:
            report(False, "site config", str(e))
            continue
        img = site["images"].get(s["image"] or "")
        if s["image"]:
            report(img and os.path.exists(img), "image %s" % s["image"], img or "not defined")
        v = dict(fmt)
        for k, val in (s.get("vars") or {}).items():
            try:
                v[k] = str(val).format(**v)
            except KeyError as e:
                report(False, "stage var %s" % k, "unknown variable %s" % e)
        paths = {}
        if s["handler"] == "edepsim":
            env = C.stage_env(site, s)
            path = re.sub(r"\$\{?PATH\}?", os.environ.get("PATH", ""), env.get("PATH", ""))
            exe = site["vars"].get("edepsim_exe", "edep-sim")
            found = shutil.which(exe, path=path or None)
            report(found, "edep-sim executable (stage PATH)", found or "%s not found" % exe)
            for lib in (env.get("LD_LIBRARY_PATH") or "").split(":"):
                if lib and "$" not in lib:
                    report(os.path.isdir(lib), "LD_LIBRARY_PATH entry", lib)
        else:
            for m_ in re.finditer(r"\{(\w+)\}", s["command"] or ""):
                if m_.group(1) in v and ("dir" in m_.group(1) or "config" in m_.group(1)):
                    paths["command {%s}" % m_.group(1)] = v[m_.group(1)]
        pc = s.get("provenance") or {}
        for kind in ("files", "software"):
            for k, pth in (pc.get(kind) or {}).items():
                try:
                    paths["provenance %s %s" % (kind, k)] = str(pth).format(**v)
                except KeyError as e:
                    report(False, "provenance %s %s" % (kind, k), "unknown variable %s" % e)
        for pth in s.get("pythonpath") or []:
            try:
                paths["pythonpath %s" % pth] = str(pth).format(**v)
            except KeyError:
                pass
        for what, pth in paths.items():
            report(os.path.exists(pth), what, pth)
        if not a.no_images and s["enabled"] and not s.get("external"):
            _check_image(site, s, report, v)
    print("all checks passed" if ok[0] else "SOME CHECKS FAILED")
    return 0 if ok[0] else 1


def _human(n):
    for u in ("B", "KB", "MB", "GB", "TB", "PB"):
        if n < 1000 or u == "PB":
            return ("%.0f %s" if u == "B" else "%.1f %s") % (n, u)
        n /= 1000.0


def cmd_destroy(a):
    c = _open(a)
    if c.tag.startswith("prod_") and not a.allow_production:
        raise CampaignError("%s is a production tag; add --allow-production to destroy it" % c.tag)
    plan, active = c.destroy_plan()
    print("destroy campaign %s (site %s) -- this deletes, it cannot be undone:" % (c.tag, c.site["name"]))
    for p, size in plan:
        print("  %10s  %s" % (_human(size), p))
    print("  %d queued/running array element(s) will be cancelled first" % len(active))
    try:
        crontab = subprocess.run(["scrontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 universal_newlines=True, timeout=30).stdout
        if c.tag in crontab:
            print("  note: your scrontab still mentions %s; remove that entry (scrontab -e)" % c.tag)
    except (OSError, subprocess.TimeoutExpired):
        pass
    if a.dry_run:
        print("[dry-run] nothing deleted")
        return
    if a.confirm is None:
        if not sys.stdin.isatty():
            raise CampaignError("not a terminal: pass --confirm %s to destroy non-interactively" % c.tag)
        a.confirm = input("type the campaign tag to confirm: ").strip()
    if a.confirm != c.tag:
        raise CampaignError("confirmation %r does not match %r; nothing deleted" % (a.confirm, c.tag))
    removed = c.destroy()
    for p, size, errors in removed:
        print("removed %s (%s)%s" % (p, _human(size),
                                     "; %d path(s) could not be removed, e.g. %s" % (len(errors), errors[0])
                                     if errors else ""))
    _HELD_LOCKS[:] = [(cc, cm) for cc, cm in _HELD_LOCKS if cc is not c]   # campaign is gone
    print("campaign %s destroyed" % c.tag)


def cmd_update_config(a):
    c = _open(a)
    changes = c.update_config(a.config, dry_run=a.dry_run)
    if not changes:
        print("no changes")
        return
    print("%s campaign config (%d change(s)):" % ("[dry-run] would update" if a.dry_run else "updated",
                                                   len(changes)))
    for ch in changes:
        print("  " + ch)
    if not a.dry_run:
        print("previous version kept as %s/campaign.yaml.<timestamp>" % c.dir)
        print("the change applies to submissions from now on; if it needs newer worker code, "
              "also run `dprod refresh-code`")


def cmd_refresh_code(a):
    c = _open(a)
    c.refresh_code()
    print("re-copied worker code to %s (affects future submissions only)" % os.path.join(c.dir, "code"))


def build_parser():
    ap = argparse.ArgumentParser(prog="dprod", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", default=os.environ.get("DPROD_SITE"),
                    help="site name (configs/sites/<name>.yaml) or path; default: the current one "
                         "(see `dprod sites`)")
    ap.add_argument("--campaign", default=os.environ.get("DPROD_CAMPAIGN"),
                    help="campaign tag; default: the current one (see `dprod campaigns`, `dprod use`)")
    sub = ap.add_subparsers(dest="cmd")
    sub.required = True        # (add_subparsers(required=...) needs python >= 3.7)

    p = sub.add_parser("init", help="create a campaign (interactive without arguments)")
    p.add_argument("config", nargs="?", help="campaign config (default: choose from configs/campaigns)")
    p.add_argument("--batch", "-y", action="store_true", help="do not ask for confirmation")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("sites", help="list site configs")
    p.set_defaults(func=cmd_sites)
    p = sub.add_parser("configs", help="list campaign configs")
    p.set_defaults(func=cmd_configs)
    p = sub.add_parser("campaigns", help="list the campaigns of a site")
    p.set_defaults(func=cmd_campaigns)
    p = sub.add_parser("use", help="set the current campaign (menu without an argument)")
    p.add_argument("tag", nargs="?")
    p.set_defaults(func=cmd_use)

    p = sub.add_parser("extend", help="grow the number of stage-1 jobs")
    p.add_argument("n_jobs", type=int, help="new total number of jobs")
    p.set_defaults(func=cmd_extend)

    for name, func, hlp in (("submit", cmd_submit, "submit new tasks whose inputs are ready"),
                            ("recover", cmd_recover, "resubmit failed tasks")):
        p = sub.add_parser(name, help=hlp)
        p.add_argument("stage", help="stage name or alias (1, 2A, 2B, 3A, 3B)")
        p.add_argument("--tasks", help="restrict to task ids, e.g. 0-99,120")
        p.add_argument("--limit", type=int, help="submit at most this many tasks")
        p.add_argument("--dry-run", action="store_true", help="write scripts, do not submit")
        _add_slurm_args(p)
        p.add_argument("--batch", "-y", action="store_true",
                       help="do not ask for confirmation (also: DPROD_BATCH=1)")
        if name == "recover":
            p.add_argument("--force", action="store_true", help="ignore max_attempts")
            p.add_argument("--reseed", action="store_true",
                           help="use new seeds for this attempt (default: identical seeds)")
        p.set_defaults(func=func)

    p = sub.add_parser("advance",
                       help="sync, then submit every ready task of every enabled stage")
    p.add_argument("--stages", help="restrict to stages, comma-separated (default: all)")
    p.add_argument("--recover", action="store_true",
                   help="also resubmit failed tasks with attempts left")
    p.add_argument("--max-queued", type=int,
                   help="per stage, keep at most this many queued+running elements")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--batch", "-y", action="store_true",
                   help="do not ask for confirmation (also: DPROD_BATCH=1)")
    _add_slurm_args(p)
    p.set_defaults(func=cmd_advance)

    p = sub.add_parser("watch", help="repeat `advance` + status until nothing can progress")
    p.add_argument("--interval", type=int, default=600, help="seconds between rounds (600)")
    p.add_argument("--stages", help="restrict to stages, comma-separated (default: all)")
    p.add_argument("--recover", action="store_true",
                   help="also resubmit failed tasks with attempts left")
    p.add_argument("--max-queued", type=int,
                   help="per stage, keep at most this many queued+running elements")
    p.add_argument("--merge-summary", action="store_true",
                   help="also update the summary HDF5 files every round")
    p.add_argument("--once", action="store_true", help="a single round (e.g. from cron)")
    _add_slurm_args(p, "rounds")
    p.add_argument("--web", action="store_true",
                   help="also write (and publish) the monitoring page every round")
    p.set_defaults(func=cmd_watch)

    p = sub.add_parser("web", help="write the monitoring page (index.html + status.json)")
    p.add_argument("--out", help="output directory (default: site web.dir, or <campaign>/web)")
    p.add_argument("--publish", action="store_true", help="run the site's web.publish command")
    p.add_argument("--no-sync", action="store_true")
    p.set_defaults(func=cmd_web)

    p = sub.add_parser("sync", help="update bookkeeping from slurm and job summaries")
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser("status", help="per-stage summary")
    p.add_argument("--no-sync", action="store_true")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("failures", help="list failed/abandoned tasks with reasons")
    p.add_argument("stage", nargs="?", default="all")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--no-sync", action="store_true")
    p.set_defaults(func=cmd_failures)

    p = sub.add_parser("tasks", help="list tasks of a stage")
    p.add_argument("stage")
    p.add_argument("--status", help="comma-separated statuses")
    p.add_argument("--tasks")
    p.set_defaults(func=cmd_tasks)

    p = sub.add_parser("mark", help="manually set task status")
    p.add_argument("stage")
    p.add_argument("tasks", help="task ids, e.g. 3,5-7")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--abandon", action="store_true", help="give up; downstream proceeds without")
    g.add_argument("--failed", action="store_true", help="mark failed (eligible for recover)")
    g.add_argument("--new", action="store_true", help="reset to new")
    p.add_argument("--note")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_mark)

    p = sub.add_parser("cancel", help="scancel array elements; their tasks go back to 'new'")
    p.add_argument("stage", nargs="?", help="stage (default: all)")
    p.add_argument("--tasks")
    p.add_argument("--queued", action="store_true", help="only elements still pending (not running)")
    p.set_defaults(func=cmd_cancel)

    p = sub.add_parser("reset-attempts",
                       help="start the max_attempts count afresh for failed tasks")
    p.add_argument("stage")
    p.add_argument("--tasks", help="task ids (default: all failed tasks of the stage)")
    p.set_defaults(func=cmd_reset_attempts)

    p = sub.add_parser("move", help="change partition/account/qos/time of queued elements in place")
    p.add_argument("stage", nargs="?", help="stage (default: all)")
    p.add_argument("--tasks")
    p.add_argument("--partition")
    p.add_argument("--account")
    p.add_argument("--qos")
    p.add_argument("--time")
    p.set_defaults(func=cmd_move)

    p = sub.add_parser("lookup", help="find files containing (job id, event id)")
    p.add_argument("job", type=int)
    p.add_argument("event", type=int)
    p.add_argument("--stage")
    p.set_defaults(func=cmd_lookup)

    p = sub.add_parser("files", help="list registered output files of a stage")
    p.add_argument("stage")
    p.add_argument("--role")
    p.add_argument("-l", "--long", action="store_true")
    p.set_defaults(func=cmd_files)

    p = sub.add_parser("check", help="preflight checks of a site (+ campaign config)")
    p.add_argument("config", nargs="?", help="campaign config to check against the site")
    p.add_argument("--no-images", action="store_true",
                   help="skip starting each stage's container (slow on first use)")
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("merge-summary",
                       help="write <campaign>_<stage>_summary.h5 for each stage from done tasks")
    p.add_argument("stage", nargs="?", help="stage(s), comma-separated (default: all)")
    p.add_argument("--rebuild", action="store_true",
                   help="rewrite the stage-1 merged file from scratch")
    p.set_defaults(func=cmd_merge_summary)

    p = sub.add_parser("destroy", help="cancel all jobs of a campaign and delete all its files")
    p.add_argument("--confirm", metavar="TAG", help="the campaign tag, to confirm without a prompt")
    p.add_argument("--dry-run", action="store_true", help="only show what would be deleted")
    p.add_argument("--allow-production", action="store_true",
                   help="required for tags starting with prod_")
    p.set_defaults(func=cmd_destroy)

    p = sub.add_parser("update-config",
                       help="replace the campaign's frozen config (checked against what already ran)")
    p.add_argument("config", help="new campaign config (same campaign tag)")
    p.add_argument("--dry-run", action="store_true", help="only show the changes")
    p.set_defaults(func=cmd_update_config)

    p = sub.add_parser("refresh-code", help="re-snapshot worker code into the campaign dir")
    p.set_defaults(func=cmd_refresh_code)
    return ap


def main(argv=None):
    a = build_parser().parse_args(argv)
    try:
        return a.func(a) or 0
    except (CampaignError, C.ConfigError, SchedulerError) as e:
        print("dprod: error: %s" % e, file=sys.stderr)
        return 1
    finally:
        while _HELD_LOCKS:
            c, cm = _HELD_LOCKS.pop()
            try:        # keep the job-side monitoring view's plan current
                from .webdata import write_plan
                write_plan(c)
            except Exception as e:
                print("warning: could not write records/plan.json: %s" % e, file=sys.stderr)
            cm.__exit__(None, None, None)


if __name__ == "__main__":
    sys.exit(main())
