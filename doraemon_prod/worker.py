"""Job-side driver: runs one task inside the job container.

    python3 -m doraemon_prod.worker --manifest <submission.json> --index <array index>

Uses only the standard library plus h5py (for id extraction), so it runs in
both the stage-1 and stage-2/3 images. It never touches the SQLite database;
the result of the attempt is a JSON summary (written atomically, always, even
on failure) that the controller ingests with `dprod sync`.

Sequence
  1. create a private work dir on local disk (site work_root, e.g. $LSCRATCH)
  2. run the stage (edep-sim, or a configured command)
  3. validate outputs and read every (job id, event id) they contain
  4. copy outputs to shared storage (temp name + atomic rename)
  5. tar the work-dir logs to storage, write the summary, clean up
"""

import argparse
import glob
import json
import os
import re
import resource
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import traceback

from . import layout
from .idreaders import IdReadError, read_ids

EDEPSIM_LOG_ERRORS = (
    "COMMAND NOT FOUND",
    "Batch is interrupted",
    "command refused",
    "illegal application state",
    "G4Exception : FatalException",
)
LOG_SUFFIXES = (".log", ".txt", ".mac", ".yaml", ".yml", ".json", ".csv", ".gdml", ".sh")


class TaskFailure(Exception):
    pass


def log(msg):
    print("[dprod-worker %s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def write_json_atomic(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def copy_atomic(src, dst):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = "%s.part.%d" % (dst, os.getpid())
    shutil.copyfile(src, tmp)
    if os.path.getsize(tmp) != os.path.getsize(src):
        os.remove(tmp)
        raise TaskFailure("size mismatch copying %s -> %s" % (src, dst))
    os.replace(tmp, dst)


_VAR_RE = re.compile(r"\$(\w+)|\$\{(\w+)\}")


def build_env(extra):
    """os.environ plus `extra`, expanding $VAR against the env being built
    (so PATH: /x/bin:$PATH prepends)."""
    env = dict(os.environ)
    for k, v in extra.items():
        env[k] = _VAR_RE.sub(lambda mo: env.get(mo.group(1) or mo.group(2), ""), v)
    return env


def run_logged(cmd, logfile, env, cwd, shell=False):
    log("running: %s" % (cmd if shell else " ".join(shlex.quote(c) for c in cmd)))
    t0 = time.time()
    with open(logfile, "w") as lf:
        rc = subprocess.call(cmd, stdout=lf, stderr=subprocess.STDOUT, env=env, cwd=cwd,
                             shell=shell, executable="/bin/bash" if shell else None)
    log("exit code %d after %.1f s (log: %s)" % (rc, time.time() - t0, os.path.basename(logfile)))
    return rc


def tail(path, n=30):
    try:
        with open(path, errors="replace") as f:
            return "".join(f.readlines()[-n:])
    except OSError:
        return ""


# --------------------------------------------------------------------------- stage 1

def edit_macro(text, g4_seed, job_id):
    """Drop any seed/run-id commands from the user macro and prepend ours."""
    drop = re.compile(r"^\s*/(random/setSeeds|random/resetEngine|edep/random/|edep/runId)")
    kept = [ln for ln in text.splitlines() if not drop.match(ln)]
    head = ["# --- inserted by doraemon_prod: reproducible seed and run id = job id ---",
            "/edep/random/randomSeed %d" % g4_seed,
            "/edep/runId %d" % job_id,
            "# --- original macro follows ---"]
    return "\n".join(head + kept) + "\n"


def edit_generator_config(text, gen_seed):
    """Set the top-level SEED of a DLPGenerator ParticleBomb yaml (-1 = clock!)."""
    pat = re.compile(r"^SEED\s*:.*$", re.M)
    if pat.search(text):
        return pat.sub("SEED: %d" % gen_seed, text)
    return "SEED: %d\n" % gen_seed + text


def run_edepsim(m, task, work, env):
    sc = m["stage_config"]
    job = task["first_job"]
    n_ev = int(sc["events_per_job"])
    in_dir = os.path.join(m["campaign_dir"], "inputs", m["stage"])
    for fn in os.listdir(in_dir):
        src = os.path.join(in_dir, fn)
        if os.path.isfile(src):
            shutil.copyfile(src, os.path.join(work, fn))

    macro = os.path.join(work, sc["macro"])
    with open(macro) as f:
        text = f.read()
    with open(macro, "w") as f:
        f.write(edit_macro(text, task["seeds"]["geant4"], job))
    if sc.get("generator_config"):
        gpath = os.path.join(work, sc["generator_config"])
        with open(gpath) as f:
            text = f.read()
        with open(gpath, "w") as f:
            f.write(edit_generator_config(text, task["seeds"]["generator"]))

    out = os.path.join(work, task["name"] + ".h5")
    exe = m["vars"].get("edepsim_exe", "edep-sim")
    cmd = [exe, "-g", sc["geometry"], "-e", str(n_ev), "-o", out, sc["macro"]]
    logfile = os.path.join(work, "edepsim.log")
    rc = run_logged(cmd, logfile, env, work)
    if rc != 0:
        raise TaskFailure("edep-sim exit code %d\n%s" % (rc, tail(logfile)))
    # edep-sim exits 0 even when a macro command fails, so scan the log as well
    with open(logfile, errors="replace") as f:
        text = f.read()
    for pat in EDEPSIM_LOG_ERRORS:
        if pat in text:
            raise TaskFailure("edep-sim log contains %r\n%s" % (pat, tail(logfile)))
    if not os.path.exists(out):
        raise TaskFailure("edep-sim produced no output file")

    ids = read_ids("edepsim", out, os.path.basename(out))
    if list(ids) != [job]:
        raise TaskFailure("output run_id(s) %s != job id %d" % (sorted(ids), job))
    if ids[job] != list(range(n_ev)):
        raise TaskFailure("expected events 0..%d, found %d events" % (n_ev - 1, len(ids[job])))

    from .provenance import write as write_provenance
    fmt = template_vars(m, task, work, work)
    auto = {"geometry": os.path.join(work, sc["geometry"]), "macro": macro}
    if sc.get("generator_config"):
        auto["generator_config"] = os.path.join(work, sc["generator_config"])
    exe_path = shutil.which(exe, path=env.get("PATH")) or exe
    rec = provenance_record(m, task, fmt, work, auto_files=auto,
                            auto_software={"edep-sim": exe_path},
                            command=" ".join(shlex.quote(c) for c in cmd))
    write_provenance(out, rec)

    from .summary import SummaryError, extract_edepsim
    try:
        tables = extract_edepsim(out)
    except (SummaryError, KeyError, ValueError) as e:
        raise TaskFailure("job summary extraction failed: %s" % e)

    rel = "%s/%s.h5" % (layout.data_rel_dir(m["stage"], job), task["name"])
    return [{"local": out, "rel": rel, "role": m["stage"], "events": ids,
             "summary_tables": tables}], []


# --------------------------------------------------------------------------- stage 2/3

class _Fmt(dict):
    def __missing__(self, key):
        raise TaskFailure("command template uses unknown variable {%s}" % key)


def template_vars(m, task, work, outdir):
    """Variables for command / provenance templates: site vars, stage vars, task info."""
    fmt = _Fmt(m["vars"])
    fmt.update(
        campaign=m["campaign"], stage=m["stage"], task_id=task["task_id"],
        first_job=task["first_job"], last_job=task["last_job"],
        n_jobs=task["last_job"] - task["first_job"] + 1,
        seed=task["seeds"]["task"], workdir=work, outdir=outdir,
        inputs=" ".join(shlex.quote(p) for p in task["inputs"]),
        input_dirs=" ".join(shlex.quote(p) for p in task["input_dirs"]),
        n_inputs=len(task["inputs"]),
    )
    for anc, paths in (task.get("extra_inputs") or {}).items():
        fmt["inputs_" + anc] = " ".join(shlex.quote(p) for p in paths)
    for k, v in (m["stage_config"].get("vars") or {}).items():
        fmt[k] = str(v).format_map(fmt)
    return fmt


def provenance_record(m, task, fmt, work, auto_files=None, auto_software=None,
                      command=None):
    """Collect /provenance for this task: automatic items + the stage's `provenance:`."""
    from .provenance import collect
    pc = m["stage_config"].get("provenance") or {}
    files = dict(auto_files or {})
    software = dict(auto_software or {})
    for dest, src in ((files, pc.get("files")), (software, pc.get("software"))):
        for name, path in (src or {}).items():
            path = str(path).format_map(fmt)
            dest[name] = path if os.path.isabs(path) else os.path.join(work, path)
    for name, path in files.items():
        if not os.path.isfile(path):
            raise TaskFailure("provenance file %s not found: %s" % (name, path))
    return collect(m, task, files=files, software=software, command=command,
                   inputs=task.get("inputs"), env=m["env"])


def run_command(m, task, work, env):
    sc = m["stage_config"]
    outdir = os.path.join(work, "out")
    os.makedirs(outdir)
    fmt = template_vars(m, task, work, outdir)
    cmd = sc["command"].format_map(fmt)
    extra_pp = [str(p).format_map(fmt) for p in (sc.get("pythonpath") or [])]
    if extra_pp:     # e.g. the pysupera checkout named in the site config runs, not another one
        env = dict(env, PYTHONPATH=":".join(extra_pp + [x for x in [env.get("PYTHONPATH", "")] if x]))
    with open(os.path.join(work, "command.sh"), "w") as f:
        # the environment the site/stage configs set, so the log shows what the command saw
        f.write("# environment set by the site/stage config:\n")
        for k in sorted(m.get("env") or {}):
            f.write("export %s=%s\n" % (k, shlex.quote(env.get(k, ""))))
        if extra_pp:
            f.write("export PYTHONPATH=%s\n" % shlex.quote(env["PYTHONPATH"]))
        f.write(cmd + "\n")
    logfile = os.path.join(work, "%s.log" % m["stage"])
    rc = run_logged(cmd, logfile, env, work, shell=True)
    if rc != 0:
        raise TaskFailure("command exit code %d\n%s" % (rc, tail(logfile)))

    paths = sorted(p for p in glob.glob(os.path.join(outdir, sc["outputs"]), recursive=True)
                   if os.path.isfile(p))
    if not paths:
        raise TaskFailure("no outputs matching %r in %s" % (sc["outputs"], outdir))

    dest = "%s/%s" % (layout.data_rel_dir(m["stage"], task["first_job"]), task["name"])
    outputs, warnings = [], []
    per_role = {}
    for p in paths:
        rel = os.path.relpath(p, outdir)
        try:
            ids = read_ids(sc["id_reader"], p, rel, sc.get("id_reader_options"))
        except IdReadError as e:
            raise TaskFailure("id extraction failed: %s" % e)
        bad = [j for j in ids if not task["first_job"] <= j <= task["last_job"]]
        if bad:
            raise TaskFailure("%s contains job ids %s outside task range %d-%d" % (
                rel, bad[:5], task["first_job"], task["last_job"]))
        role = rel.split(os.sep)[0] if os.sep in rel else m["stage"]
        n = per_role.setdefault(role, set())
        for j, evs in ids.items():
            for e in evs:
                if (j, e) in n:
                    raise TaskFailure("event (%d, %d) appears twice in role %s" % (j, e, role))
                n.add((j, e))
        outputs.append({"local": p, "rel": "%s/%s" % (dest, rel), "role": role, "events": ids})

    expected = task.get("expected_events")
    for role, evs in per_role.items():
        if expected is not None and len(evs) != expected:
            msg = "role %s has %d events, inputs had %d" % (role, len(evs), expected)
            if sc.get("strict_events"):
                raise TaskFailure(msg)
            warnings.append(msg)
    # provenance goes into the outputs of the configured roles (default: all)
    from .provenance import write as write_provenance
    roles = (sc.get("provenance") or {}).get("roles")
    rec = provenance_record(m, task, fmt, work, command=cmd)
    targets = [o for o in outputs if roles is None or o["role"] in roles]
    if roles and not targets:
        raise TaskFailure("no output of role(s) %s to hold provenance" % roles)
    sources = task.get("provenance_sources") or []
    for o in targets:
        # upstream blocks: parent files holding any of this output's jobs
        jobs = set(o["events"])
        upstream = [src["path"] for src in sources
                    if any(src["first_job"] <= j <= src["last_job"] for j in jobs)]
        if sources and not upstream:
            raise TaskFailure("no parent file with provenance for jobs %s of %s" % (
                sorted(jobs)[:5], o["rel"]))
        write_provenance(o["local"], rec, upstream, log=log)
    # a retry must not leave stale files from an earlier attempt in the task dir
    old = os.path.join(m["campaign_dir"], dest)
    if os.path.isdir(old):
        log("removing stale output dir %s" % old)
        shutil.rmtree(old)
    return outputs, warnings


HANDLERS = {"edepsim": run_edepsim, "command": run_command}


# --------------------------------------------------------------------------- main

def tar_logs(work, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    tmp = dest + ".part.%d" % os.getpid()
    with tarfile.open(tmp, "w:gz") as tf:
        for root, _, files in os.walk(work):
            for fn in files:
                p = os.path.join(root, fn)
                if fn.endswith(LOG_SUFFIXES) and os.path.getsize(p) < 200 * 1024 * 1024:
                    tf.add(p, arcname=os.path.relpath(p, os.path.dirname(work)))
    os.replace(tmp, dest)


class ResourceMonitor(threading.Thread):
    """Samples memory (and GPU) use of this job at a fixed interval.

    CPU RAM : summed RSS of this process and all its descendants (/proc).
    GPU     : nvidia-smi utilization.gpu and memory.used of the GPUs given to
              the job (CUDA_VISIBLE_DEVICES), averaged / summed over them.
    Averages are time-weighted (trapezoid) over the monitored period.
    """

    def __init__(self, interval=10.0, gpu=False):
        super().__init__(daemon=True)
        self.interval = max(1.0, float(interval))
        self.gpu = gpu
        self.gpu_note = None
        self.samples = []            # (t, rss_mb, gpu_util_pct, gpu_mem_used_mb)
        self.gpu_total_mb = None
        self.n_gpus = 0
        self._stop_evt = threading.Event()
        self._lock = threading.Lock()
        self.root = os.getpid()

    def _tree_rss_mb(self):
        children = {}
        for d in os.listdir("/proc"):
            if not d.isdigit():
                continue
            try:
                with open("/proc/%s/stat" % d) as f:
                    # field 4 is ppid; comm (field 2) may contain spaces -> split after ')'
                    ppid = int(f.read().rsplit(")", 1)[1].split()[1])
            except (OSError, ValueError, IndexError):
                continue
            children.setdefault(ppid, []).append(int(d))
        total_kb, todo = 0, [self.root]
        while todo:
            pid = todo.pop()
            todo.extend(children.get(pid, []))
            try:
                with open("/proc/%d/status" % pid) as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            total_kb += int(line.split()[1])
                            break
            except (OSError, ValueError):
                pass
        return total_kb / 1024.0

    def _gpu(self):
        cmd = ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total",
               "--format=csv,noheader,nounits"]
        cvd = os.environ.get("CUDA_VISIBLE_DEVICES", "").strip()
        attempts = [cmd + ["-i", cvd], cmd] if cvd and cvd != "NoDevFiles" else [cmd]
        for c in attempts:
            try:
                out = subprocess.run(c, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     universal_newlines=True, timeout=20)
            except (OSError, subprocess.TimeoutExpired) as e:
                self.gpu_note = "nvidia-smi failed: %s" % e
                continue
            if out.returncode != 0:
                self.gpu_note = "nvidia-smi exit code %d" % out.returncode
                continue
            rows = [[float(x) for x in ln.split(",")] for ln in out.stdout.splitlines()
                    if ln.strip() and "N/A" not in ln]
            if rows:
                self.n_gpus = len(rows)
                self.gpu_total_mb = sum(r[2] for r in rows)
                self.gpu_note = None
                return sum(r[0] for r in rows) / len(rows), sum(r[1] for r in rows)
        return None, None

    def sample(self):
        util = mem = None
        if self.gpu:
            util, mem = self._gpu()
            if util is None and not self.samples:
                self.gpu = False     # unavailable from the start: stop trying
        with self._lock:
            self.samples.append((time.time(), self._tree_rss_mb(), util, mem))

    def run(self):
        while not self._stop_evt.is_set():
            try:
                self.sample()
            except Exception as e:  # never let monitoring kill the job
                log("resource monitor error: %s" % e)
            self._stop_evt.wait(self.interval)

    def stop(self):
        self._stop_evt.set()
        self.join(timeout=30)
        try:
            self.sample()            # final point, so the average covers the whole job
        except Exception:
            pass

    @staticmethod
    def _avg(ts, vs):
        pts = [(t, v) for t, v in zip(ts, vs) if v is not None]
        if not pts:
            return None
        if len(pts) == 1 or pts[-1][0] <= pts[0][0]:
            return sum(v for _, v in pts) / len(pts)
        area = sum((t1 - t0) * (v0 + v1) / 2 for (t0, v0), (t1, v1) in zip(pts, pts[1:]))
        return area / (pts[-1][0] - pts[0][0])

    def result(self):
        with self._lock:
            smp = list(self.samples)
        ts = [x[0] for x in smp]
        out = {"n_samples": len(smp), "interval_s": self.interval,
               "avg_rss_mb": self._avg(ts, [x[1] for x in smp]),
               "max_tree_rss_mb": max((x[1] for x in smp), default=None)}
        if any(x[2] is not None for x in smp) or self.gpu_note:
            mems = [x[3] for x in smp if x[3] is not None]
            out.update({
                "n_gpus": self.n_gpus,
                "gpu_util_pct": self._avg(ts, [x[2] for x in smp]),
                "gpu_mem_used_mb": self._avg(ts, [x[3] for x in smp]),
                "gpu_mem_max_mb": max(mems) if mems else None,
                "gpu_mem_total_mb": self.gpu_total_mb,
                "gpu_note": self.gpu_note})
        return out


def max_rss_mb():
    """Peak resident memory of this process and of its largest finished child (MB)."""
    kb = max(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
             resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    return kb / 1024.0


def write_job_summary_file(m, task, summary, tables, work, monitor):
    from .summary import write_job_summary
    events, particles = tables
    now = time.time()
    res = monitor.result()
    info = {"job_id": task["first_job"], "attempt": task["attempt"],
            "start_time": summary["start"], "duration_s": now - summary["start"],
            "node": summary["host"],
            "max_rss_mb": max(max_rss_mb(), res.get("max_tree_rss_mb") or 0),
            "avg_rss_mb": res.get("avg_rss_mb"), "gpu_util_pct": res.get("gpu_util_pct"),
            "gpu_mem_used_mb": res.get("gpu_mem_used_mb"),
            "seed_geant4": task["seeds"].get("geant4", -1),
            "seed_generator": task["seeds"].get("generator", -1)}
    local = os.path.join(work, "job_summary.h5")
    write_job_summary(local, info, events, particles)
    rel = layout.job_summary_rel_path(m["stage"], task["name"], task["attempt"])
    copy_atomic(local, os.path.join(m["campaign_dir"], rel))
    summary["job_summary"] = rel


def run_task(m, index, work_root):
    task = m["tasks"][index]
    stage, name, attempt = m["stage"], task["name"], task["attempt"]
    cdir = m["campaign_dir"]
    summary_path = os.path.join(cdir, layout.summary_rel_path(stage, name, attempt))
    summary = {
        "campaign": m["campaign"], "stage": stage, "task_id": task["task_id"],
        "attempt": attempt, "submission_id": m["submission_id"], "array_index": index,
        "first_job": task["first_job"], "last_job": task["last_job"],
        "host": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_array_job_id": os.environ.get("SLURM_ARRAY_JOB_ID"),
        "seeds": task["seeds"], "start": time.time(), "status": "failed",
        "expected_events": task.get("expected_events"),
        "n_input_jobs": task.get("n_input_jobs"),
        "reason": None, "files": [], "warnings": [], "n_events": 0,
    }
    work = None
    mon_cfg = m.get("monitor") or {}
    monitor = ResourceMonitor(mon_cfg.get("interval", 10), mon_cfg.get("gpu", False))
    monitor.start()
    web = m.get("web") or {}
    try:     # job-side monitoring record; never fatal
        from .webdata import write_record
        write_record(cdir, stage, task, "running", summary, m.get("time_limit_s"))
    except Exception as e:
        log("warning: could not write monitoring record: %s" % e)
    log("campaign %s stage %s task %d (%s) attempt %d on %s" % (
        m["campaign"], stage, task["task_id"], name, attempt, summary["host"]))
    try:
        work_root = os.path.expandvars(work_root or "/tmp")
        if "$" in work_root:
            raise TaskFailure("work_root %r: undefined variable on this node" % work_root)
        os.makedirs(work_root, exist_ok=True)
        work = os.path.join(work_root, "dprod_%s_%s_a%02d_%s" % (
            m["campaign"], name, attempt, os.environ.get("SLURM_JOB_ID", os.getpid())))
        if os.path.exists(work):
            shutil.rmtree(work)
        os.makedirs(work)
        write_json_atomic(os.path.join(work, "task.json"), {"task": task, "manifest": {
            k: v for k, v in m.items() if k != "tasks"}})
        with open(os.path.join(work, "env.txt"), "w") as f:
            f.write("".join("%s=%s\n" % kv for kv in sorted(os.environ.items())))

        env = build_env(m["env"])
        outputs, warnings = HANDLERS[m["stage_config"]["handler"]](m, task, work, env)
        summary["warnings"] = warnings
        for w in warnings:
            log("WARNING: " + w)

        for o in outputs:
            copy_atomic(o["local"], os.path.join(cdir, o["rel"]))
            summary["files"].append({"path": o["rel"], "role": o["role"],
                                     "size": os.path.getsize(o["local"]),
                                     "events": {str(j): e for j, e in o["events"].items()}})
        for o in outputs:
            if "summary_tables" in o:
                write_job_summary_file(m, task, summary, o["summary_tables"], work, monitor)
        roles = {}
        for f in summary["files"]:
            roles[f["role"]] = roles.get(f["role"], 0) + sum(len(e) for e in f["events"].values())
        # events per task = events in the most complete role (sensor/step/hits all count once)
        summary["n_events"] = max(roles.values()) if roles else 0
        summary["status"] = "ok"
        log("success: %d file(s), %d event(s)" % (len(summary["files"]), summary["n_events"]))
    except TaskFailure as e:
        summary["reason"] = str(e)
        log("FAILED: %s" % e)
    except Exception as e:
        summary["reason"] = "worker exception: %s: %s" % (type(e).__name__, e)
        log("FAILED with exception\n" + traceback.format_exc())
    finally:
        summary["end"] = time.time()
        summary["wall_s"] = summary["end"] - summary["start"]
        monitor.stop()
        res = monitor.result()
        summary["resources"] = res
        # peak: largest single process (getrusage) or largest sampled process-tree sum
        summary["max_rss_mb"] = max(max_rss_mb(), res.get("max_tree_rss_mb") or 0)
        log("resources: " + ", ".join("%s=%s" % (k, ("%.1f" % v) if isinstance(v, float) else v)
                                      for k, v in sorted(res.items())))
        if work and os.path.isdir(work):
            try:
                tar_logs(work, os.path.join(cdir, layout.logs_rel_path(stage, name, attempt)))
            except Exception as e:
                log("could not archive logs: %s" % e)
            if summary["status"] == "ok" or not m.get("keep_failed_workdir"):
                shutil.rmtree(work, ignore_errors=True)
        write_json_atomic(summary_path, summary)
        log("summary written to %s" % summary_path)
        try:     # job-side monitoring: final record + snapshot page; never fatal
            from .webdata import rebuild_from_job, write_record
            write_record(cdir, stage, task, "done" if summary["status"] == "ok" else "failed",
                         summary, m.get("time_limit_s"))
            if web.get("dir") and web.get("enabled", True):
                rebuild_from_job(cdir, web["dir"], web.get("job_rebuild_s", 300), log=log,
                                 base=web.get("base"))
        except Exception as e:
            log("warning: monitoring snapshot not updated: %s" % e)
    return 0 if summary["status"] == "ok" else 1


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--index", type=int,
                    default=int(os.environ.get("SLURM_ARRAY_TASK_ID", "-1")))
    ap.add_argument("--work-root", default=None)
    a = ap.parse_args(argv)
    with open(a.manifest) as f:
        m = json.load(f)
    if not 0 <= a.index < len(m["tasks"]):
        log("array index %d out of range (manifest has %d tasks)" % (a.index, len(m["tasks"])))
        return 2
    return run_task(m, a.index, a.work_root or m.get("work_root"))


if __name__ == "__main__":
    sys.exit(main())
