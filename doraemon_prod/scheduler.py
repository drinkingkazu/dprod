"""Batch scheduler backends (controller side).

SlurmScheduler  sbatch / sacct / scancel
LocalScheduler  runs array elements sequentially on this machine, emulating the
                slurm environment variables; for development and tests.

query() returns {(array_job_id, index): info} where info has keys
state (slurm-style, e.g. PENDING RUNNING COMPLETED FAILED TIMEOUT ...),
exit_code, elapsed_s, start, end, node.
"""

import datetime
import json
import os
import re
import subprocess
import time

# scheduler states after which the element will not run again
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL",
            "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED", "SPECIAL_EXIT"}
RUNNING = {"RUNNING", "COMPLETING", "CONFIGURING", "STAGE_OUT", "SIGNALING"}


class SchedulerError(Exception):
    pass


def _run(cmd):
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True)
    except FileNotFoundError:
        raise SchedulerError("command not found: %s (are you on a slurm login node?)" % cmd[0])
    if p.returncode != 0:
        raise SchedulerError("%s failed (%d): %s" % (" ".join(cmd), p.returncode, p.stderr.strip()))
    return p.stdout


def _parse_time(s):
    if not s or s in ("Unknown", "None", "N/A"):
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").timestamp()
    except ValueError:
        return None


def _expand_indices(spec):
    """'[0-3,7,9-10%5]' -> [0,1,2,3,7,9,10]"""
    spec = spec.strip("[]").split("%")[0]
    out = []
    for part in spec.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")[:2]
            out.extend(range(int(a), int(b.split(":")[0]) + 1))
        else:
            out.append(int(part))
    return out


class SlurmScheduler:
    name = "slurm"

    def __init__(self, site):
        self.site = site

    def submit(self, script, n_tasks, max_concurrent=None):
        array = "0-%d" % (n_tasks - 1)
        if max_concurrent:
            array += "%%%d" % int(max_concurrent)
        out = _run(["sbatch", "--parsable", "--array=" + array, script])
        return out.strip().split(";")[0]

    def query(self, array_job_ids):
        result = {}
        ids = sorted(set(array_job_ids))
        for i in range(0, len(ids), 200):
            chunk = ids[i:i + 200]
            out = _run(["sacct", "-X", "-P", "-n", "-j", ",".join(chunk),
                        "--format=JobID,State,ExitCode,ElapsedRaw,Start,End,NodeList"])
            for line in out.splitlines():
                f = line.split("|")
                if len(f) < 7 or "_" not in f[0]:
                    continue
                jid, idx = f[0].split("_", 1)
                state = f[1].split()[0] if f[1] else "UNKNOWN"
                info = {"state": state, "exit_code": f[2],
                        "elapsed_s": float(f[3]) if f[3].isdigit() else None,
                        "start": _parse_time(f[4]), "end": _parse_time(f[5]),
                        "node": f[6] if f[6] not in ("None assigned", "") else None}
                if idx.startswith("["):
                    for k in _expand_indices(idx):
                        result[(jid, k)] = dict(info)
                elif idx.isdigit():
                    result[(jid, int(idx))] = info
        return result

    def cancel(self, array_job_id, indices=None):
        if indices:
            targets = ["%s_%d" % (array_job_id, i) for i in indices]
        else:
            targets = [array_job_id]
        _run(["scancel"] + targets)

    def update(self, array_job_id, indices, fields):
        """scontrol update of pending elements (all pending ones if indices is None)."""
        args = ["%s=%s" % kv for kv in fields.items()]
        targets = [array_job_id] if not indices else ["%s_%d" % (array_job_id, i) for i in indices]
        for t in targets:
            _run(["scontrol", "update", "JobId=%s" % t] + args)


class LocalScheduler:
    """Runs every array element immediately and sequentially (blocking)."""
    name = "local"

    def __init__(self, site, state_dir):
        self.site = site
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)

    def _state_path(self, jid):
        return os.path.join(self.state_dir, "%s.json" % jid)

    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1000) % 10**10)
        text = open(script).read()
        m = re.search(r"^#SBATCH --output=(\S+)", text, re.M)
        out_pat = m.group(1) if m else os.path.join(self.state_dir, "slurm-%A_%a.out")
        states = {}
        for idx in range(n_tasks):
            env = dict(os.environ, SLURM_ARRAY_JOB_ID=jid, SLURM_ARRAY_TASK_ID=str(idx),
                       SLURM_JOB_ID="%s%03d" % (jid, idx))
            out = out_pat.replace("%A", jid).replace("%a", str(idx))
            os.makedirs(os.path.dirname(out), exist_ok=True)
            t0 = time.time()
            with open(out, "w") as f:
                rc = subprocess.call(["bash", script], stdout=f, stderr=subprocess.STDOUT, env=env)
            t1 = time.time()
            states[str(idx)] = {"state": "COMPLETED" if rc == 0 else "FAILED",
                                "exit_code": "%d:0" % rc, "elapsed_s": t1 - t0,
                                "start": t0, "end": t1, "node": os.uname().nodename}
            with open(self._state_path(jid), "w") as f:
                json.dump(states, f)
        return jid

    def query(self, array_job_ids):
        result = {}
        for jid in set(array_job_ids):
            p = self._state_path(jid)
            if os.path.exists(p):
                for idx, info in json.load(open(p)).items():
                    result[(jid, int(idx))] = info
        return result

    def cancel(self, array_job_id, indices=None):
        pass

    def update(self, array_job_id, indices, fields):
        pass


def get_scheduler(site, campaign_dir):
    if site["scheduler"] == "local":
        return LocalScheduler(site, os.path.join(campaign_dir, "local_sched"))
    return SlurmScheduler(site)
