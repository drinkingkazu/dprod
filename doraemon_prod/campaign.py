"""Campaign operations (controller side): init, define, submit, sync, mark."""

import contextlib
import errno
import fcntl
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time

from . import config as C
from . import db as D
from . import layout as L
from .scheduler import RUNNING, TERMINAL, get_scheduler

PKG_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(C.REPO_DIR, "templates", "sbatch.sh.in")


class CampaignError(Exception):
    pass


def _span(ids):
    if not ids:
        return ""
    if ids == list(range(ids[0], ids[-1] + 1)):
        return "%d-%d" % (ids[0], ids[-1]) if len(ids) > 1 else str(ids[0])
    return ",".join(map(str, ids[:10])) + (",..." if len(ids) > 10 else "")


def campaign_dir(site, tag):
    return os.path.join(site["storage_root"], tag)


def _db_path(site, cdir, tag):
    if site.get("db_path"):
        return site["db_path"].format(campaign=tag)
    return os.path.join(cdir, "bookkeeping.sqlite")


def _snapshot_code(cdir):
    dest = os.path.join(cdir, "code", "doraemon_prod")
    if os.path.exists(dest):
        shutil.rmtree(dest)
    shutil.copytree(PKG_DIR, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*~"))


def _snapshot_inputs(cdir, cfg, src_base):
    for s in cfg["stages"].values():
        if not s["input_dir"] or s.get("external"):
            continue
        src = s["input_dir"]
        if not os.path.isabs(src):
            src = os.path.join(src_base, src)
        if not os.path.isdir(src):
            raise CampaignError("stage %s: input_dir %s not found" % (s["name"], src))
        dest = os.path.join(cdir, "inputs", s["name"])
        os.makedirs(dest, exist_ok=True)
        for fn in sorted(os.listdir(src)):
            p = os.path.join(src, fn)
            if os.path.isfile(p) and not fn.endswith("~") and not fn.startswith("."):
                shutil.copy2(p, os.path.join(dest, fn))
        for key in ("geometry", "macro", "generator_config"):
            if s[key] and not os.path.exists(os.path.join(dest, s[key])):
                raise CampaignError("stage %s: %s %s not in %s" % (s["name"], key, s[key], src))


class Campaign:
    def __init__(self, site, tag):
        self.site = site
        self.tag = tag
        self.dir = campaign_dir(site, tag)
        cfg_path = os.path.join(self.dir, "campaign.yaml")
        if not os.path.exists(cfg_path):
            raise CampaignError("campaign %s is not initialized at %s" % (tag, self.dir))
        self.cfg = C.load_campaign(cfg_path, site)
        self.db_path = _db_path(site, self.dir, tag)
        self.con = D.connect(self.db_path)
        self.sched = get_scheduler(site, self.dir)
        # one-off slurm option overrides for submissions made by this process
        # (e.g. dprod submit 1 --partition roma --account X)
        self.slurm_override = {}

    # ------------------------------------------------------------------ locking
    @contextlib.contextmanager
    def lock(self, timeout=900, out=print):
        """Exclusive campaign lock for commands that change bookkeeping.

        Prevents e.g. a background `dprod watch` (or scrontab round) and a manual
        `dprod submit` from submitting the same task twice. Waits up to `timeout` s.
        POSIX locks are per process: two Campaign objects in one process do not
        exclude each other (not needed; commands are separate processes).
        """
        path = os.path.join(self.dir, ".dprod.lock")
        f = open(path, "a+")
        t0, warned, locked = time.time(), False, False
        try:
            while True:
                try:
                    # POSIX (lockf) locks: honored across hosts on parallel filesystems,
                    # so a scrontab round on a compute node and a manual command on
                    # the login node exclude each other (flock is often host-local)
                    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                    break
                except OSError as e:
                    if e.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                        out("warning: cannot lock %s (%s); continuing without lock" % (path, e))
                        break
                    if time.time() - t0 > timeout:
                        f.seek(0)
                        raise CampaignError("campaign is locked by another dprod: %s"
                                            % f.read().strip())
                    if not warned:
                        f.seek(0)
                        out("waiting for campaign lock held by: %s" % f.read().strip())
                        warned = True
                    time.sleep(2)
            if locked:
                f.seek(0)
                f.truncate()
                f.write("pid %d on %s since %s\n" % (os.getpid(), socket.gethostname(),
                                                   time.strftime("%Y-%m-%d %H:%M:%S")))
                f.flush()
            yield
        finally:
            if locked:
                fcntl.lockf(f, fcntl.LOCK_UN)
            f.close()

    # ------------------------------------------------------------------ init
    @classmethod
    def init(cls, site, cfg_path):
        raw = C.load_yaml(cfg_path)
        if C.needs_materialize(raw):
            raw = C.materialize_inherit(raw, site, source=cfg_path)
        cfg = C.normalize_campaign(raw, source=cfg_path)
        tag = cfg["campaign"]
        cdir = campaign_dir(site, tag)
        if os.path.exists(os.path.join(cdir, "campaign.yaml")):
            raise CampaignError("campaign %s already exists at %s" % (tag, cdir))
        for s in cfg["stages"].values():
            if s["enabled"] and not s["external"]:   # validate site/stage compatibility early
                C.slurm_options(site, s)
                C.container_prefix(site, s)
        os.makedirs(cdir, exist_ok=True)
        _snapshot_inputs(cdir, cfg, C.REPO_DIR)
        _snapshot_code(cdir)
        if raw.get("inherit"):              # frozen config: inherited stages resolved
            import yaml
            with open(os.path.join(cdir, "campaign.yaml"), "w") as f:
                f.write("# Frozen by `dprod init` from %s; stages marked external are inherited\n"
                        "# (read-only) from campaign %s.\n" % (os.path.abspath(cfg_path),
                                                              raw["inherit"]["campaign"]))
                yaml.safe_dump(raw, f, sort_keys=False, default_flow_style=False)
        else:
            shutil.copyfile(cfg_path, os.path.join(cdir, "campaign.yaml"))
        dbp = _db_path(site, cdir, tag)
        con = D.create(dbp, {"campaign": tag, "site": site["name"], "created": D.now(),
                             "config_source": os.path.abspath(cfg_path)})
        con.close()
        c = cls(site, tag)
        root = c.cfg["stages"][c.cfg["root_stage"]]
        if root["external"]:
            c.sync_external()                  # import the source's tasks, define ours
        else:
            c.extend(int(root["n_jobs"]))
        from .webdata import write_plan
        write_plan(c)
        return c

    def extend(self, n_jobs):
        """Grow the root stage to n_jobs jobs, then define downstream tasks."""
        root = self.cfg["root_stage"]
        if self.cfg["stages"][root]["external"]:
            raise CampaignError("the jobs of this campaign come from campaign %s; extend that one "
                                "(new jobs are picked up here automatically)"
                                % self.cfg["stages"][root]["external"])
        cur = self.con.execute("SELECT COUNT(*) FROM tasks WHERE stage = ?", (root,)).fetchone()[0]
        if n_jobs < cur:
            raise CampaignError("campaign already has %d jobs" % cur)
        with self.con:
            D.add_tasks(self.con, root, [(j, j, j, None, None) for j in range(cur, n_jobs)])
            for name, s in self.cfg["stages"].items():
                if s["parent"]:
                    self._define_downstream(s)
        return n_jobs - cur

    def _define_downstream(self, s):
        parents = D.tasks(self.con, s["parent"])
        row = self.con.execute("SELECT MAX(last_parent), MAX(task_id) FROM tasks WHERE stage = ?",
                               (s["name"],)).fetchone()
        next_parent = (row[0] + 1) if row[0] is not None else 0
        next_id = (row[1] + 1) if row[1] is not None else 0
        by_id = {p["task_id"]: p for p in parents}
        rows = []
        while next_parent in by_id:
            last = next_parent
            while last + 1 in by_id and last + 1 - next_parent < s["merge"]:
                last += 1
            rows.append((next_id, by_id[next_parent]["first_job"], by_id[last]["last_job"],
                         next_parent, last))
            next_id += 1
            next_parent = last + 1
        D.add_tasks(self.con, s["name"], rows)

    # fields that define tasks / job and event ids: frozen once a stage has attempts
    FROZEN_STAGE_KEYS = ("handler", "parent", "merge", "events_per_job", "input_dir",
                         "geometry", "macro", "generator_config")

    def update_config(self, path, dry_run=False, out=print):
        """Replace the frozen campaign config with `path`, refusing changes that would
        break what already ran. Returns the list of changes."""
        new = C.load_campaign(path, self.site)
        cur = self.cfg
        if new["campaign"] != self.tag:
            raise CampaignError("config is for campaign %r, not %r" % (new["campaign"], self.tag))
        ran = {st: self.con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ?", (st,)).fetchone()[0]
               for st in cur["stages"]}
        errors, changes = [], []
        gone = [st for st in cur["stages"] if st not in new["stages"]]
        if gone:
            errors.append("stages cannot be removed: %s (disable them with enabled: false)" % ", ".join(gone))
        if new["root_stage"] != cur["root_stage"]:
            errors.append("the root stage cannot change")
        if new["seed"] != cur["seed"] and any(ran.values()):
            errors.append("seed: %s -> %s refused: jobs already ran with seeds derived from it"
                          % (cur["seed"], new["seed"]))
        for key in ("description", "max_attempts", "seed"):
            if new.get(key) != cur.get(key):
                changes.append("%s: %r -> %r" % (key, cur.get(key), new.get(key)))

        def flat(d, prefix=""):
            outd = {}
            for k, v in (d or {}).items():
                if isinstance(v, dict):
                    outd.update(flat(v, prefix + k + "."))
                else:
                    outd[prefix + k] = v
            return outd

        for st, ns in new["stages"].items():
            if ns["external"] or (cur["stages"].get(st) or {}).get("external"):
                if (cur["stages"].get(st) or {}).get("external") != ns["external"]:
                    errors.append("%s: inherited stages cannot be added or changed" % st)
                continue                     # definitions come from the source campaign
            if st not in cur["stages"]:
                if ns["parent"] not in cur["stages"] and ns["parent"] not in new["stages"]:
                    errors.append("new stage %s: unknown parent %s" % (st, ns["parent"]))
                changes.append("%s: new stage" % st)
                continue
            cs = cur["stages"][st]
            a, b = flat(cur["raw_stages"][st]), flat(new["raw_stages"][st])
            for k in sorted(set(a) | set(b)):
                if a.get(k) == b.get(k):
                    continue
                top = k.split(".")[0]
                if top == "input_dir" and ran[st] and self._same_inputs(st, ns):
                    changes.append("%s.input_dir: %r -> %r (moved; same file contents)"
                                   % (st, a.get(k), b.get(k)))
                elif top in self.FROZEN_STAGE_KEYS and ran[st]:
                    errors.append("%s.%s: %r -> %r refused: stage %s already ran (%d attempt(s))"
                                  % (st, k, a.get(k), b.get(k), st, ran[st]))
                elif top == "n_jobs":
                    changes.append("%s.n_jobs: %r -> %r ignored (the job count is set with `dprod extend`)"
                                   % (st, a.get(k), b.get(k)))
                else:
                    changes.append("%s.%s: %r -> %r" % (st, k, a.get(k), b.get(k)))
            if not ran[st] and (cs["merge"] != ns["merge"] or cs["parent"] != ns["parent"]):
                below = [d for d in new["stages"] if self._descends(new, d, st)]
                if any(ran.get(d) for d in below):
                    errors.append("%s: merge/parent change needs its downstream stages unrun" % st)
        if errors:
            raise CampaignError("config not updated:\n  " + "\n  ".join(errors))
        if dry_run or not changes:
            return changes
        stamp = time.strftime("%Y%m%d_%H%M%S")
        cfg_file = os.path.join(self.dir, "campaign.yaml")
        shutil.copyfile(cfg_file, cfg_file + "." + stamp)
        raw_new = C.load_yaml(path)
        if C.needs_materialize(raw_new):
            import yaml
            raw_new = C.materialize_inherit(raw_new, self.site, source=path)
            with open(cfg_file, "w") as f:
                yaml.safe_dump(raw_new, f, sort_keys=False, default_flow_style=False)
        else:
            shutil.copyfile(path, cfg_file)
        self.cfg = C.load_campaign(cfg_file, self.site)
        with self.con:
            # re-define tasks of unrun stages whose grouping changed, and add new stages
            for st, ns in self.cfg["stages"].items():
                if not ns["parent"]:
                    continue
                old_s = cur["stages"].get(st)
                if old_s is None or (not ran.get(st) and (old_s["merge"] != ns["merge"] or
                                                          old_s["parent"] != ns["parent"])):
                    self.con.execute("DELETE FROM tasks WHERE stage = ?", (st,))
                    self._define_downstream(ns)
            D.set_meta(self.con, "config_updated", stamp)
        # stage-1 inputs are re-snapshotted only if stage 1 never ran
        if not ran.get(self.cfg["root_stage"]):
            _snapshot_inputs(self.dir, self.cfg, C.REPO_DIR)
        return changes

    def _same_inputs(self, stage, new_stage):
        """True if the new input_dir holds the same geometry/macro/generator files
        (byte for byte) as the campaign's snapshot of the stage inputs."""
        src = new_stage["input_dir"]
        if not os.path.isabs(src):
            src = os.path.join(C.REPO_DIR, src)
        snap = os.path.join(self.dir, "inputs", stage)
        for key in ("geometry", "macro", "generator_config"):
            name = new_stage.get(key)
            if not name:
                continue
            a, b = os.path.join(snap, name), os.path.join(src, name)
            if not (os.path.isfile(a) and os.path.isfile(b)):
                return False
            with open(a, "rb") as fa, open(b, "rb") as fb:
                if fa.read() != fb.read():
                    return False
        return True

    @staticmethod
    def _descends(cfg, stage, ancestor):
        p = cfg["stages"][stage]["parent"]
        while p:
            if p == ancestor:
                return True
            p = cfg["stages"][p]["parent"]
        return False

    def sync_external(self):
        """Import tasks, files and (job, event) records of inherited stages from their
        source campaign(s), incrementally (tasks updated since the last import), then
        define this campaign's downstream tasks on top of them."""
        ext = [s for s in self.cfg["stages"].values() if s["external"]]
        if not ext:
            return 0
        import sqlite3
        n = 0
        by_src = {}
        for s in ext:
            by_src.setdefault((s["external"], s["external_dir"]), []).append(s)
        with self.con:
            for (tag, src_dir), stages in by_src.items():
                dbp = _db_path(self.site, src_dir, tag)
                try:
                    src = sqlite3.connect("file:%s?mode=ro" % dbp, uri=True, timeout=60)
                except sqlite3.Error as e:
                    raise CampaignError("cannot read campaign %s bookkeeping %s: %s" % (tag, dbp, e))
                src.row_factory = sqlite3.Row
                try:
                    for s in stages:
                        key = "ext_mark:%s" % s["name"]
                        mark = float(D.get_meta(self.con, key, 0))
                        new_mark = mark
                        for t in src.execute("SELECT * FROM tasks WHERE stage = ? AND updated > ?"
                                             " ORDER BY task_id", (s["name"], mark)):
                            self.con.execute(
                                "INSERT OR REPLACE INTO tasks (stage, task_id, first_job, last_job,"
                                " first_parent, last_parent, status, n_attempts, n_events, note, updated)"
                                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (s["name"], t["task_id"], t["first_job"], t["last_job"], t["first_parent"],
                                 t["last_parent"], t["status"], t["n_attempts"], t["n_events"],
                                 "from campaign %s" % tag, D.now()))
                            files = []
                            if t["status"] == D.DONE:
                                for f in src.execute("SELECT * FROM files WHERE stage = ? AND task_id = ?",
                                                     (s["name"], t["task_id"])).fetchall():
                                    evs = {}
                                    for e in src.execute("SELECT job_id, event_id FROM events WHERE file_id = ?",
                                                         (f["id"],)):
                                        evs.setdefault(e[0], []).append(e[1])
                                    path = f["path"] if os.path.isabs(f["path"]) else os.path.join(src_dir, f["path"])
                                    files.append({"path": path, "role": f["role"], "size": f["size"],
                                                  "events": evs})
                            D.replace_task_outputs(self.con, s["name"], t["task_id"], files)
                            new_mark = max(new_mark, t["updated"] or 0)
                            n += 1
                        D.set_meta(self.con, key, new_mark)
                finally:
                    src.close()
            for s in self.cfg["stages"].values():
                if s["parent"] and not s["external"]:
                    self._define_downstream(s)
        return n

    def refresh_code(self):
        _snapshot_code(self.dir)
        with self.con:
            D.set_meta(self.con, "code_refreshed", D.now())

    # ------------------------------------------------------------------ readiness
    def parent_state(self, s, task):
        """For a downstream task: (ready, done_parent_rows, reason)."""
        parents = self.con.execute(
            "SELECT * FROM tasks WHERE stage = ? AND task_id BETWEEN ? AND ? ORDER BY task_id",
            (s["parent"], task["first_parent"], task["last_parent"])).fetchall()
        waiting = [p for p in parents if p["status"] not in D.TERMINAL_STATES]
        done = [p for p in parents if p["status"] == D.DONE]
        if waiting:
            return False, done, "%d parent task(s) not finished" % len(waiting)
        if not done:
            return False, done, "all parent tasks abandoned"
        return True, done, None

    def _task_entry(self, s, t, attempt, reseed):
        seed = self.cfg["seed"]
        keys = (attempt,) if reseed else ()
        e = {"task_id": t["task_id"], "attempt": attempt,
             "first_job": t["first_job"], "last_job": t["last_job"],
             "name": L.task_name(s["name"], t["first_job"], t["last_job"]),
             "seeds": {"task": L.derive_seed(seed, s["name"], t["task_id"], *keys)},
             "inputs": [], "input_dirs": [], "expected_events": None}
        if not s["parent"]:
            e["seeds"]["geant4"] = L.derive_seed(seed, "geant4", t["first_job"], *keys)
            e["seeds"]["generator"] = L.derive_seed(seed, "generator", t["first_job"], *keys)
            return e
        ready, done, reason = self.parent_state(s, t)
        if not ready:
            return None
        ps = self.cfg["stages"][s["parent"]]
        roles = s.get("input_roles")
        prov_roles = (ps.get("provenance") or {}).get("roles")
        dirs = []
        e["provenance_sources"] = []    # parent files carrying /provenance, with job ranges
        for p in done:
            for f in D.task_files(self.con, ps["name"], p["task_id"]):
                if prov_roles is None or f["role"] in prov_roles:
                    e["provenance_sources"].append({
                        "path": os.path.join(self.dir, f["path"]),
                        "first_job": f["first_job"], "last_job": f["last_job"]})
                if roles and f["role"] not in roles:
                    continue
                e["inputs"].append(os.path.join(self.dir, f["path"]))
            if ps["handler"] == "command":
                dirs.append(os.path.join(ps["external_dir"] or self.dir,
                                         L.data_rel_dir(ps["name"], p["first_job"]),
                                         L.task_name(ps["name"], p["first_job"], p["last_job"])))
            else:
                dirs.extend(os.path.dirname(x) for x in e["inputs"])
        e["input_dirs"] = sorted(set(dirs))
        e["extra_inputs"] = {}
        for anc in s["also_inputs"]:
            rows = self.con.execute(
                "SELECT f.path FROM files f JOIN tasks t ON t.stage = f.stage AND t.task_id = f.task_id"
                " WHERE f.stage = ? AND t.status = 'done' AND f.first_job >= ? AND f.last_job <= ?"
                " ORDER BY f.first_job, f.role, f.path", (anc, t["first_job"], t["last_job"]))
            e["extra_inputs"][anc] = [os.path.join(self.dir, r["path"]) for r in rows]
        e["expected_events"] = sum(p["n_events"] or 0 for p in done)
        ids = [p["task_id"] for p in done]
        e["n_input_jobs"] = self.con.execute(
            "SELECT COUNT(DISTINCT e.job_id) FROM events e JOIN files f ON f.id = e.file_id"
            " WHERE f.stage = ? AND f.task_id IN (%s)" % ",".join("?" * len(ids)),
            [ps["name"]] + ids).fetchone()[0]
        if not e["inputs"]:
            return None
        return e

    # ------------------------------------------------------------------ submit
    def candidates(self, stage, recovery=False, task_ids=None, force=False, reseed=False):
        s = self.cfg["stages"][stage]
        if recovery:
            rows = D.tasks(self.con, stage, [D.FAILED], task_ids)
            if not force:
                rows = [r for r in rows
                        if self.attempts_used(stage, r["task_id"]) < self.cfg["max_attempts"]]
            # tasks put back to 'new' by a cancellation were submitted before: recover
            # resubmits them too (they have not used an attempt)
            rows += [r for r in D.tasks(self.con, stage, [D.NEW], task_ids) if r["n_attempts"] > 0]
            rows.sort(key=lambda r: r["task_id"])
        else:
            rows = D.tasks(self.con, stage, [D.NEW], task_ids)
        out = []
        for r in rows:
            e = self._task_entry(s, r, r["n_attempts"] + 1, reseed)
            if e is not None:
                out.append(e)
        return out

    def plan(self, stage, recovery=False, task_ids=None, limit=None, force=False, reseed=False):
        """The task entries a submit would send (nothing is submitted)."""
        s = self.cfg["stages"][stage]
        if s["external"]:
            raise CampaignError("stage %s comes from campaign %s (read-only here)" % (stage, s["external"]))
        if not s["enabled"]:
            raise CampaignError("stage %s is disabled (enabled: false in the campaign config)" % stage)
        entries = self.candidates(stage, recovery, task_ids, force, reseed)
        return entries[:limit] if limit is not None else entries

    def plan_advance(self, stages=None, recover=False, max_queued=None):
        """What `advance` would submit now: [(stage, recovery, entries)], nothing submitted."""
        self.sync()
        items = []
        for stage in stages or list(self.cfg["stages"]):
            s = self.cfg["stages"][stage]
            if not s["enabled"] or s["external"]:
                continue
            cap = max_queued if max_queued is not None else s["max_queued"]
            n = 0
            for recovery in ((False, True) if recover else (False,)):
                limit = None
                if cap is not None:
                    limit = max(0, int(cap) - self.n_active(stage) - n)
                    if limit == 0:
                        break
                entries = self.plan(stage, recovery=recovery, limit=limit)
                if entries:
                    items.append((stage, recovery, entries))
                    n += len(entries)
        return items

    def plan_summary(self, items):
        """Rows describing planned submissions: tasks, arrays, jobs, events, slurm settings."""
        rows = []
        n = int(self.site["max_array_size"])
        for stage, recovery, entries in items:
            s = self.cfg["stages"][stage]
            opts = self.slurm_opts(s)
            root = not s["parent"]
            jobs = sorted(set(j for e in entries for j in range(e["first_job"], e["last_job"] + 1)))
            if root:
                events = len(jobs) * int(s["events_per_job"] or 0)
            else:
                events = sum(e.get("expected_events") or 0 for e in entries)
            rows.append({
                "stage": stage, "alias": s["alias"] or stage,
                "kind": "retry" if recovery else "new", "tasks": len(entries),
                "arrays": (len(entries) + n - 1) // n, "jobs": len(jobs),
                "job_range": "%d-%d" % (jobs[0], jobs[-1]) if jobs else "",
                "events": events,
                "partition": opts.get("partition") or opts.get("constraint") or "",
                "account": opts.get("account") or "", "qos": opts.get("qos") or "",
                "time": opts.get("time") or "", "gpus": opts.get("gpus") or "",
            })
        return rows

    def submit(self, stage, recovery=False, task_ids=None, limit=None, dry_run=False,
               force=False, reseed=False, entries=None):
        s = self.cfg["stages"][stage]
        if entries is None:
            entries = self.plan(stage, recovery, task_ids, limit, force, reseed)
        n = int(self.site["max_array_size"])
        chunks = [entries[i:i + n] for i in range(0, len(entries), n)]
        results = []
        for chunk in chunks:
            results.append(self._submit_chunk(s, chunk, "recovery" if recovery else "normal",
                                              dry_run))
        return results

    def n_active(self, stage):
        return self.con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state IN"
                                " ('submitted', 'running')", (stage,)).fetchone()[0]

    def advance(self, stages=None, recover=False, max_queued=None, dry_run=False, out=print,
                plan=None):
        """Sync, then submit every ready task of every enabled stage (in stage order).

        recover     also resubmit failed tasks that have attempts left
        max_queued  per-stage cap on queued+running array elements (overrides the
                    stage's `max_queued`); None = no cap
        plan        submit exactly this plan_advance() result instead of re-planning
        Returns {stage: number of tasks submitted}.
        """
        submitted = {}
        if plan is not None:
            for stage, recovery, entries in plan:
                for r in self.submit(stage, recovery=recovery, dry_run=dry_run, entries=entries):
                    submitted[stage] = submitted.get(stage, 0) + r["n_tasks"]
                    out("%s%s: %s %d task(s) [%s]%s" % (
                        "[dry-run] " if dry_run else "", stage,
                        "resubmitted" if recovery else "submitted", r["n_tasks"],
                        _span(r["task_ids"]), "" if dry_run else " as array %s" % r["array_job_id"]))
            return submitted
        for stage in stages or list(self.cfg["stages"]):
            s = self.cfg["stages"][stage]
            if not s["enabled"]:
                continue
            self.sync()       # cheap; lets a stage see what finished upstream just now
            cap = max_queued if max_queued is not None else s["max_queued"]
            n = 0
            for recovery in ((False, True) if recover else (False,)):
                limit = None
                if cap is not None:
                    limit = max(0, int(cap) - self.n_active(stage) - (n if dry_run else 0))
                    if limit == 0:
                        break
                for r in self.submit(stage, recovery=recovery, limit=limit, dry_run=dry_run):
                    n += r["n_tasks"]
                    ids = r["task_ids"]
                    out("%s%s: %s %d task(s) [%s]%s" % (
                        "[dry-run] " if dry_run else "", stage,
                        "resubmitted" if recovery else "submitted", r["n_tasks"],
                        _span(ids), "" if dry_run else " as array %s" % r["array_job_id"]))
            submitted[stage] = n
        return submitted

    def progress(self):
        """Counts over enabled stages: active attempts, ready-but-unsubmitted, retryable."""
        active = ready = retry = 0
        for stage, s in self.cfg["stages"].items():
            if not s["enabled"] or s["external"]:
                continue
            active += self.n_active(stage)
            ready += len(self.candidates(stage))
            retry += len(self.candidates(stage, recovery=True))
        return {"active": active, "ready": ready, "retryable": retry}

    def _web_settings(self):
        from .web import web_base, web_dir
        web = self.site.get("web") or {}
        return {"dir": web_dir(self), "base": web_base(self.site),
                "job_rebuild_s": int(web.get("job_rebuild_s", 300)),
                "enabled": bool(web.get("job_snapshot", True))}

    def _manifest(self, s, sub_id, entries):
        return {
            "campaign": self.tag, "site": self.site["name"], "stage": s["name"],
            "submission_id": sub_id, "campaign_dir": self.dir,
            "code_dir": os.path.join(self.dir, "code"),
            "work_root": self.site["work_root"],
            "stage_config": s, "stage_config_raw": self.cfg["raw_stages"][s["name"]],
            "vars": self.site["vars"], "env": C.stage_env(self.site, s),
            "keep_failed_workdir": bool(self.site.get("keep_failed_workdir")),
            "monitor": C.monitor_settings(self.site, s),
            "image": self.site["images"].get(s["image"] or "", ""),
            "time_limit_s": C.slurm_time_s(self.slurm_opts(s).get("time")),
            "slurm_override": dict(self.slurm_override),
            "web": self._web_settings(),
            "tasks": entries,
        }

    def slurm_opts(self, s):
        opts = C.slurm_options(self.site, s)
        opts.update(self.slurm_override)
        return opts

    def _script(self, s, manifest_path):
        opts = self.slurm_opts(s)
        extra = opts.pop("extra", None) or []
        image = self.site["images"].get(s["image"] or "", "")
        lines = []
        for k, v in opts.items():
            flag = "--" + k.replace("_", "-")
            if v is False and k == "requeue":
                # PREEMPTED is then final and handled by `dprod recover`; with
                # requeue slurm would silently re-run the same attempt instead.
                lines.append("#SBATCH --no-requeue")
                continue
            if v is None or v is False or v == "":
                continue
            lines.append("#SBATCH %s" % flag if v is True else "#SBATCH %s=%s" % (flag, v))
        for x in extra:
            lines.append("#SBATCH %s" % str(x).format(image=image))
        logdir = os.path.join(self.site["log_root"], self.tag, s["name"])
        os.makedirs(logdir, exist_ok=True)
        prefix = C.container_prefix(self.site, s)
        worker = "env PYTHONPATH=%s python3 -m doraemon_prod.worker --manifest %s" % (
            shlex.quote(os.path.join(self.dir, "code")), shlex.quote(manifest_path))
        subs = {
            "JOB_NAME": "%s.%s" % (self.tag, s["alias"] or s["name"]),
            "LOG": os.path.join(logdir, "slurm-%A_%a.out"),
            "DIRECTIVES": "\n".join(lines),
            "WORK_ROOT": self.site["work_root"],
            "RUN": (prefix + " " if prefix else "") + worker,
            "CAMPAIGN": self.tag, "STAGE": s["name"], "MANIFEST": manifest_path,
        }
        text = open(TEMPLATE).read()
        for k, v in subs.items():
            text = text.replace("@%s@" % k, str(v))
        return text

    def _submit_chunk(self, s, entries, kind, dry_run):
        subdir = os.path.join(self.dir, "submissions", s["name"])
        os.makedirs(subdir, exist_ok=True)
        with self.con:
            cur = self.con.execute(
                "INSERT INTO submissions (stage, kind, site, n_tasks, manifest, script, submit_time)"
                " VALUES (?, ?, ?, ?, '', '', ?)",
                (s["name"], kind, self.site["name"], len(entries), D.now()))
            sub_id = cur.lastrowid
        base = os.path.join(subdir, "sub_%05d" % sub_id)
        mpath, spath = base + ".json", base + ".sh"
        with open(mpath, "w") as f:
            json.dump(self._manifest(s, sub_id, entries), f, indent=1)
        with open(spath, "w") as f:
            f.write(self._script(s, mpath))
        res = {"submission_id": sub_id, "n_tasks": len(entries), "script": spath,
               "task_ids": [e["task_id"] for e in entries], "array_job_id": None}
        if dry_run:
            with self.con:
                self.con.execute("DELETE FROM submissions WHERE id = ?", (sub_id,))
            res["dry_run"] = True
            return res
        # Record attempts *before* submitting so a crash here never loses track of
        # running jobs; on sbatch failure roll them back.
        with self.con:
            self.con.execute("UPDATE submissions SET manifest = ?, script = ? WHERE id = ?",
                             (mpath, spath, sub_id))
            for idx, e in enumerate(entries):
                self.con.execute(
                    "INSERT INTO attempts (stage, task_id, attempt, submission_id, array_index,"
                    " state, summary_path) VALUES (?, ?, ?, ?, ?, 'submitted', ?)",
                    (s["name"], e["task_id"], e["attempt"], sub_id, idx,
                     L.summary_rel_path(s["name"], e["name"], e["attempt"])))
                self.con.execute(
                    "UPDATE tasks SET status = 'submitted', n_attempts = ?, updated = ?"
                    " WHERE stage = ? AND task_id = ?",
                    (e["attempt"], D.now(), s["name"], e["task_id"]))
        try:
            jid = self.sched.submit(spath, len(entries), s["max_concurrent"])
        except Exception:
            with self.con:
                self.con.execute("DELETE FROM attempts WHERE submission_id = ?", (sub_id,))
                self.con.execute("DELETE FROM submissions WHERE id = ?", (sub_id,))
                for e in entries:
                    self.con.execute(
                        "UPDATE tasks SET status = ?, n_attempts = ? WHERE stage = ? AND task_id = ?",
                        (D.NEW if e["attempt"] == 1 else D.FAILED, e["attempt"] - 1,
                         s["name"], e["task_id"]))
            raise
        with self.con:
            self.con.execute("UPDATE submissions SET array_job_id = ? WHERE id = ?", (jid, sub_id))
        res["array_job_id"] = jid
        return res

    # ------------------------------------------------------------------ sync
    def job_log_path(self, a):
        """The slurm log of an attempt (row with stage, array_job_id, array_index)."""
        return os.path.join(self.site["log_root"], self.tag, a["stage"],
                            "slurm-%s_%s.out" % (a["array_job_id"], a["array_index"]))

    def _log_excerpt(self, a, n=6):
        """': <last error-looking line>' + the log tail, for failures without a worker
        summary (e.g. the container never started), so `dprod failures` shows why."""
        path = self.job_log_path(a)
        try:
            with open(path, errors="replace") as f:
                lines = [ln.rstrip() for ln in f.readlines()[-200:] if ln.strip()]
        except OSError:
            return " (job log %s not found)" % path
        rc = None
        for ln in lines:
            if ln.startswith("doraemon_prod: finished") and "exit code" in ln:
                rc = ln.rsplit("exit code", 1)[1].strip()
        body = [ln for ln in lines if not ln.startswith("doraemon_prod: ")]
        if not body:
            return "; job log has no error output (%s)" % path
        key = next((ln for ln in reversed(body) if any(w in ln for w in (
            "FATAL", "ERROR", "Error", "error:", "No such file", "not found", "Killed",
            "denied", "Traceback", "CANCELLED", "oom"))), body[-1])
        return "%s: %s\n  job log %s:\n    %s" % (
            "; job exit code %s" % rc if rc else "", key.strip()[:300], path,
            "\n    ".join(ln[:300] for ln in body[-n:]))

    def _fix_legacy_cancellations(self):
        """Attempts recorded as failed only because slurm cancelled them (versions
        before cancellations stopped counting): make them 'cancelled' and put their
        tasks back to 'new', so they do not use up max_attempts."""
        rows = self.con.execute(
            "SELECT * FROM attempts WHERE state = 'failed' AND sched_state = 'CANCELLED'"
            " AND reason LIKE 'scheduler state CANCELLED%'").fetchall()
        if not rows:
            return 0
        with self.con:
            for a in rows:
                self.con.execute("UPDATE attempts SET state = 'cancelled', reason = ?"
                                 " WHERE stage = ? AND task_id = ? AND attempt = ?",
                                 ("cancelled (was counted as a failure before)",
                                  a["stage"], a["task_id"], a["attempt"]))
                t = self.con.execute("SELECT * FROM tasks WHERE stage = ? AND task_id = ?",
                                     (a["stage"], a["task_id"])).fetchone()
                if t["n_attempts"] == a["attempt"] and t["status"] == D.FAILED:
                    D.set_task_status(self.con, a["stage"], a["task_id"], D.NEW,
                                      note="attempt %d cancelled" % a["attempt"])
        return len(rows)

    def reset_attempts(self, stage, task_ids=None):
        """Start the max_attempts count afresh for failed tasks (history is kept:
        their earlier attempts are flagged as not counted)."""
        rows = D.tasks(self.con, stage, [D.FAILED], task_ids)
        with self.con:
            for t in rows:
                self.con.execute("UPDATE attempts SET uncounted = 1 WHERE stage = ? AND task_id = ?",
                                 (stage, t["task_id"]))
        return [t["task_id"] for t in rows]

    def sync(self):
        """Update attempts/tasks from the scheduler and worker summaries."""
        self._fix_legacy_cancellations()
        self.sync_external()
        rows = self.con.execute(
            "SELECT a.*, s.array_job_id FROM attempts a JOIN submissions s ON s.id = a.submission_id"
            " WHERE a.state IN ('submitted', 'running')").fetchall()
        if not rows:
            return {}
        info = self.sched.query([r["array_job_id"] for r in rows if r["array_job_id"]])
        counts = {}
        with self.con:
            for r in rows:
                q = info.get((r["array_job_id"], r["array_index"]))
                new = self._sync_attempt(r, q)
                counts[new] = counts.get(new, 0) + 1
        return counts

    def _sync_attempt(self, a, q):
        key = (a["stage"], a["task_id"], a["attempt"])
        if q:
            self.con.execute(
                "UPDATE attempts SET sched_state = ?, exit_code = ?, node = ?, start_time = ?,"
                " end_time = ?, elapsed_s = ? WHERE stage = ? AND task_id = ? AND attempt = ?",
                (q["state"], q["exit_code"], q["node"], q["start"], q["end"], q["elapsed_s"]) + key)
        spath = os.path.join(self.dir, a["summary_path"])
        state, reason, summary = None, None, None
        if os.path.exists(spath):
            try:
                with open(spath) as f:
                    summary = json.load(f)
            except (OSError, ValueError) as e:
                state, reason = "failed", "unreadable summary: %s" % e
            if summary is not None:
                state = "done" if summary.get("status") == "ok" else "failed"
                reason = summary.get("reason")
        elif q and q["state"] == "CANCELLED":
            # cancelled by a person (scancel, dprod cancel) or an admin: not a failure,
            # does not count toward max_attempts; the task can simply be submitted again
            state = "cancelled"
            reason = "cancelled (scheduler state CANCELLED, exit %s)" % q["exit_code"]
        elif q and q["state"] in TERMINAL:
            state = "failed"
            reason = "scheduler state %s (exit %s), no worker summary" % (q["state"], q["exit_code"])
            reason += self._log_excerpt(a)
        elif q and q["state"] in RUNNING:
            state = "running"
        else:
            return a["state"]

        summary = summary or {}
        res = summary.get("resources") or {}
        self.con.execute(
            "UPDATE attempts SET state = ?, reason = ?, wall_s = COALESCE(?, wall_s),"
            " max_rss_mb = COALESCE(?, max_rss_mb), avg_rss_mb = COALESCE(?, avg_rss_mb),"
            " gpu_util_pct = COALESCE(?, gpu_util_pct), gpu_mem_used_mb = COALESCE(?, gpu_mem_used_mb),"
            " gpu_mem_max_mb = COALESCE(?, gpu_mem_max_mb),"
            " start_time = COALESCE(start_time, ?), end_time = COALESCE(end_time, ?)"
            " WHERE stage = ? AND task_id = ? AND attempt = ?",
            (state, reason, summary.get("wall_s"), summary.get("max_rss_mb"),
             res.get("avg_rss_mb"), res.get("gpu_util_pct"), res.get("gpu_mem_used_mb"),
             res.get("gpu_mem_max_mb"), summary.get("start"), summary.get("end")) + key)
        task = self.con.execute("SELECT * FROM tasks WHERE stage = ? AND task_id = ?",
                                key[:2]).fetchone()
        if task["n_attempts"] != a["attempt"] or task["status"] == D.ABANDONED:
            return state          # superseded attempt: bookkeeping only
        if state == "done":
            D.replace_task_outputs(self.con, a["stage"], a["task_id"], summary["files"])
            note = "; ".join(summary.get("warnings") or [])   # "" clears a failed attempt's note
            D.set_task_status(self.con, a["stage"], a["task_id"], D.DONE, note=note,
                              n_events=summary.get("n_events"))
        elif state == "failed":
            D.set_task_status(self.con, a["stage"], a["task_id"], D.FAILED,
                              note=(reason or "")[:2000])
        elif state == "running":
            D.set_task_status(self.con, a["stage"], a["task_id"], D.RUNNING)
        elif state == "cancelled":
            D.set_task_status(self.con, a["stage"], a["task_id"], D.NEW,
                              note="attempt %d cancelled" % a["attempt"])
        return state

    # ------------------------------------------------------------------ job summary
    def merged_summary_path(self, stage):
        return os.path.join(self.dir, "%s_%s_summary.h5" % (self.tag, stage))

    def merge_summary(self, stages=None, rebuild=False, out=print):
        """Write the summary HDF5 of each stage from its done tasks.

        root stage : merge per-job summary HDF5 files (job/event/particle tables),
                     appending only new jobs unless rebuild
        downstream : rebuild a /job table (one row per task) from JSON summaries
        Runs `python3 -m doraemon_prod.summary` in the stage's container (needs h5py).
        Returns {stage: list of done task ids lacking a summary}.
        """
        missing = {}
        for stage in stages or list(self.cfg["stages"]):
            s = self.cfg["stages"][stage]
            if s["external"]:
                out("%s: from campaign %s (its summary is there), skipped" % (stage, s["external"]))
                continue
            done = D.tasks(self.con, stage, [D.DONE])
            if not done:
                out("%s: no done tasks, skipped" % stage)
                continue
            root = stage == self.cfg["root_stage"]
            lines, miss = [], []
            for t in done:
                name = L.task_name(stage, t["first_job"], t["last_job"])
                rel = (L.job_summary_rel_path if root else L.summary_rel_path)(
                    stage, name, t["n_attempts"])
                path = os.path.join(self.dir, rel)
                if not os.path.exists(path):
                    miss.append(t["task_id"])
                elif root:
                    lines.append("%d %d %s" % (t["first_job"], t["n_attempts"], path))
                else:
                    lines.append(path)
            missing[stage] = miss
            lst = os.path.join(self.dir, "summaries", "%s_merge_list.txt" % stage)
            os.makedirs(os.path.dirname(lst), exist_ok=True)
            with open(lst, "w") as f:
                f.write("".join(x + "\n" for x in lines))
            prefix = C.container_prefix(self.site, dict(s, container_flags=""), login=True)
            cmd = "%s env PYTHONPATH=%s python3 -m doraemon_prod.summary %s --list %s" \
                  " --output %s --campaign %s --stage %s%s" % (
                      prefix, shlex.quote(C.REPO_DIR), "merge" if root else "tasks",
                      shlex.quote(lst), shlex.quote(self.merged_summary_path(stage)),
                      self.tag, stage, " --rebuild" if rebuild and root else "")
            sys.stdout.flush()           # keep log order when stdout is a file (cron rounds)
            rc = subprocess.call(cmd.strip(), shell=True)
            if rc != 0:
                raise CampaignError("summary merge failed (exit %d): %s" % (rc, cmd))
        return missing

    # ------------------------------------------------------------------ destroy
    def destroy_plan(self):
        """Paths `destroy` would remove: [(path, bytes)], plus the active attempts."""
        from .web import web_dir
        paths = [self.dir, os.path.join(self.site["log_root"], self.tag)]
        wd = web_dir(self)
        if not os.path.realpath(wd).startswith(os.path.realpath(self.dir) + os.sep) and \
                self.tag in os.path.realpath(wd).split(os.sep):
            paths.append(wd)          # a separate, campaign-specific web directory
        if self.site.get("db_path"):
            paths.append(self.db_path)
        out = []
        for p in paths:
            if not os.path.exists(p):
                continue
            size = 0
            if os.path.isdir(p):
                for root, _, files in os.walk(p):
                    for fn in files:
                        try:
                            size += os.lstat(os.path.join(root, fn)).st_size
                        except OSError:
                            pass
            else:
                size = os.path.getsize(p)
            out.append((p, size))
        self.sync()
        return out, self._selected_active(None, None, queued_only=False)

    def destroy(self, wait_s=180, out=print):
        """Cancel the campaign's jobs and delete its directories (see destroy_plan)."""
        storage = os.path.realpath(self.site["storage_root"])
        cdir = os.path.realpath(self.dir)
        # identity checks: never remove anything that is not this campaign's
        if os.path.dirname(cdir) != storage or os.path.basename(cdir) != self.tag or \
                not os.path.exists(os.path.join(cdir, "campaign.yaml")):
            raise CampaignError("refusing: %s does not look like campaign %s" % (cdir, self.tag))
        plan, active = self.destroy_plan()
        if active:
            out("cancelling %d queued/running element(s) ..." % len(active))
            self.cancel()
            t0 = time.time()
            while time.time() - t0 < wait_s:
                self.sync()
                left = self._selected_active(None, None, queued_only=False)
                if not left:
                    break
                time.sleep(10)
            else:
                out("warning: %d element(s) still active after %d s; deleting anyway" % (
                    len(left), wait_s))
        self.con.close()
        try:
            from .web import web_base
            from .webdata import update_registry, REGISTRY
            base = web_base(self.site)
            if os.path.exists(os.path.join(base, REGISTRY)):
                from .web import write_hub
                write_hub(base, update_registry(base, self.tag, remove=True))
        except Exception as e:
            out("warning: could not remove %s from the campaign list: %s" % (self.tag, e))
        removed = []
        for p, size in plan:
            rp = os.path.realpath(p)
            if rp in ("/", storage, os.path.realpath(self.site["log_root"])) or \
                    self.tag not in rp.split(os.sep) and rp != os.path.realpath(self.db_path):
                out("skipped (not campaign-specific): %s" % p)
                continue
            errors = []
            if os.path.isdir(p) and not os.path.islink(p):
                shutil.rmtree(p, onerror=lambda f, path, e: errors.append(path))
            else:
                os.remove(p)
            removed.append((p, size, errors))
        return removed

    # ------------------------------------------------------------------ manual marks
    def mark(self, stage, task_ids, status, note=None, force=False):
        if status not in (D.ABANDONED, D.FAILED, D.NEW):
            raise CampaignError("can only mark tasks abandoned, failed or new")
        if self.cfg["stages"][stage]["external"]:
            raise CampaignError("stage %s comes from campaign %s; mark it there"
                                % (stage, self.cfg["stages"][stage]["external"]))
        changed = 0
        with self.con:
            for t in D.tasks(self.con, stage, task_ids=task_ids):
                if t["status"] in D.ACTIVE_STATES and not force:
                    raise CampaignError("task %s/%d is %s; cancel it first or use --force" % (
                        stage, t["task_id"], t["status"]))
                if t["status"] == D.DONE and not force:
                    raise CampaignError("task %s/%d is done; use --force to re-mark it" % (
                        stage, t["task_id"]))
                D.set_task_status(self.con, stage, t["task_id"], status,
                                  note=note or "marked %s manually" % status)
                changed += 1
        return changed

    def active_arrays(self, stage=None):
        q = ("SELECT DISTINCT s.array_job_id, a.array_index, a.stage, a.task_id FROM attempts a"
             " JOIN submissions s ON s.id = a.submission_id WHERE a.state IN ('submitted','running')")
        args = []
        if stage:
            q += " AND a.stage = ?"
            args.append(stage)
        return self.con.execute(q, args).fetchall()

    def attempts_used(self, stage, task_id):
        """Attempts that count toward max_attempts (cancellations by the user do not)."""
        return self.con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND task_id = ?"
                                " AND state != 'cancelled' AND COALESCE(uncounted, 0) = 0",
                                (stage, task_id)).fetchone()[0]

    def _selected_active(self, stage, task_ids, queued_only):
        states = ("submitted",) if queued_only else ("submitted", "running")
        q = ("SELECT a.*, s.array_job_id FROM attempts a JOIN submissions s ON s.id = a.submission_id"
             " WHERE a.state IN (%s)" % ",".join("?" * len(states)))
        args = list(states)
        if stage:
            q += " AND a.stage = ?"
            args.append(stage)
        rows = self.con.execute(q + " ORDER BY a.stage, a.task_id", args).fetchall()
        if task_ids is not None:
            rows = [r for r in rows if r["task_id"] in set(task_ids)]
        if queued_only:   # the attempt may have started since the last sync
            rows = [r for r in rows if (r["sched_state"] or "PENDING") == "PENDING"]
        return rows

    def cancel(self, stage=None, task_ids=None, queued_only=False):
        """scancel active array elements; their tasks go back to 'new' (ready to be
        submitted again, e.g. with other slurm settings) and the cancelled attempt
        does not count toward max_attempts."""
        self.sync()
        rows = self._selected_active(stage, task_ids, queued_only)
        by_array = {}
        for r in rows:
            by_array.setdefault(r["array_job_id"], []).append(r["array_index"])
        for jid, idx in by_array.items():
            self.sched.cancel(jid, idx)
        with self.con:
            for r in rows:
                self.con.execute(
                    "UPDATE attempts SET state = 'cancelled', reason = 'cancelled by user'"
                    " WHERE stage = ? AND task_id = ? AND attempt = ?",
                    (r["stage"], r["task_id"], r["attempt"]))
                D.set_task_status(self.con, r["stage"], r["task_id"], D.NEW,
                                  note="attempt %d cancelled by user" % r["attempt"])
        return rows

    MOVABLE = {"partition": "Partition", "account": "Account", "qos": "QOS", "time": "TimeLimit"}

    def move(self, stage, changes, task_ids=None):
        """Change partition/account/qos/time of queued (pending) array elements in
        place (scontrol update): no resubmission, no attempt used."""
        bad = [k for k in changes if k not in self.MOVABLE]
        if bad:
            raise CampaignError("move can change only %s (not %s); use cancel --queued + submit "
                                "for other options" % (", ".join(self.MOVABLE), ", ".join(bad)))
        self.sync()
        rows = self._selected_active(stage, task_ids, queued_only=True)
        fields = {self.MOVABLE[k]: v for k, v in changes.items()}
        by_array = {}
        for r in rows:
            by_array.setdefault(r["array_job_id"], []).append(r["array_index"])
        for jid, idx in by_array.items():
            n_pending = self.con.execute(
                "SELECT COUNT(*) FROM attempts a JOIN submissions s ON s.id = a.submission_id"
                " WHERE s.array_job_id = ? AND a.state = 'submitted'", (jid,)).fetchone()[0]
            self.sched.update(jid, None if len(idx) == n_pending else idx, fields)
        with open(os.path.join(self.dir, "submissions", "moves.log"), "a") as f:
            f.write("%s  %s  %d element(s)  %s  arrays %s\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), stage or "all", len(rows),
                " ".join("%s=%s" % kv for kv in changes.items()), ",".join(by_array)))
        return rows
