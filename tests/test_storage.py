"""Test the storage budget (campaign config `storage:`): probe jobs, per-task estimates,
downstream-first allocation, cancelling pending elements, submit refusal, min_free.

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_storage.py <scratch dir>

Jobs never run: elements stay PENDING, and the test "finishes" tasks by registering
outputs of known size in the bookkeeping, as a sync of a successful job would.
"""
import contextlib
import io
import json
import os
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import campaign as K  # noqa: E402
from doraemon_prod import cli, config as C, db as D  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402
from doraemon_prod.storage import parse_size  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(ROOT)
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""
os.environ["DPROD_BATCH"] = "1"
G = 1024 ** 3

assert parse_size("40T") == 40 * 1024 ** 4 and parse_size("1.5G") == int(1.5 * G)
assert parse_size("500GB") == 500 * G and parse_size(1000) == 1000 and parse_size(None) is None


class PendingScheduler(LocalScheduler):
    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        json.dump({str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None,
                            "start": None, "end": None, "node": None} for i in range(n_tasks)},
                  open(self._state_path(jid), "w"))
        return jid

    def cancel(self, array_job_id, indices=None):
        p = self._state_path(array_job_id)
        states = json.load(open(p))
        for i in indices:
            states[str(i)]["state"] = "CANCELLED"
        json.dump(states, open(p, "w"))


K.get_scheduler = lambda site, cdir: PendingScheduler(site, os.path.join(cdir, "local_sched"))

site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, ROOT), 1))
TAG = "storagetest"


def config(storage):
    path = os.path.join(ROOT, "campaign.yaml")
    t = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
    t = t.replace("campaign: localtest_doraemon_v0.0", "campaign: " + TAG).replace("n_jobs: 6", "n_jobs: 20")
    open(path, "w").write(t + "\nstorage:\n" + "".join("  %s: %s\n" % kv for kv in storage.items()))
    return path


def dprod(*args, rc=0):
    print("+ dprod " + " ".join(args))
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        got = cli.main(["--site", site, "--campaign", TAG] + list(args))
    print(out.getvalue(), end="")
    assert got == rc, (got, rc)
    return out.getvalue()


def camp():
    return K.Campaign(C.load_site(site), TAG)


def states(stage):
    return {t["task_id"]: t["status"] for t in D.tasks(camp().con, stage)}


def finish(stage, task_ids, size):
    """Register task outputs of `size` bytes, as the sync of a successful attempt does."""
    c = camp()
    with c.con:
        for tid in task_ids:
            t = c.con.execute("SELECT * FROM tasks WHERE stage = ? AND task_id = ?", (stage, tid)).fetchone()
            c.con.execute("UPDATE attempts SET state = 'done', end_time = ? WHERE stage = ? AND task_id = ?"
                          " AND attempt = ?", (time.time(), stage, tid, t["n_attempts"]))
            events = {str(j): [0, 1] for j in range(t["first_job"], t["last_job"] + 1)}
            D.replace_task_outputs(c.con, stage, tid, [{
                "path": "data/%s/%s.h5" % (stage, L_name(stage, t)), "role": "out", "size": size,
                "events": events}])
            D.set_task_status(c.con, stage, tid, D.DONE, n_events=2 * len(events))


def L_name(stage, t):
    from doraemon_prod import layout as L
    return L.task_name(stage, t["first_job"], t["last_job"])


def count(st, status):
    return sum(1 for v in st.values() if v == status)


dprod("init", config({"limit": "10G", "probe_jobs": 2}))

# 1. no estimate yet: only probe_jobs stage-1 tasks go out
out = dprod("advance")
assert count(states("edepsim"), "submitted") == 2, states("edepsim")
assert "probing" in dprod("status")
print("OK: 2 probe jobs before any size estimate")

# 2. probes finish with 1G each -> estimate 1.1G; used 2G, room 8G -> 7 more stage-1 tasks.
#    2A task 0 (jobs 0-1) is ready and has no estimate: it gets a probe, downstream first.
finish("edepsim", [0, 1], 1 * G)
out = dprod("advance")
s1 = states("edepsim")
assert count(s1, "done") == 2 and count(s1, "submitted") == 7, s1
assert states("jaxtpc_wire")[0] == "submitted", states("jaxtpc_wire")
assert "limited to 7 of 18" in out, out
print("OK: stage 1 limited to what fits (7 x 1.1G in 8G); 2A probe submitted")

# 3. the limit is lowered: advance cancels pending stage-1 elements, highest ids first,
#    and submits nothing new
dprod("update-config", config({"limit": "5G", "probe_jobs": 2}))
out = dprod("advance")
s1 = states("edepsim")
# 5G - 2G used = 3G: room for 2 reserved elements (2.2G); 5 of the 7 are cancelled
assert [s1[i] for i in range(2, 9)] == ["submitted"] * 2 + ["new"] * 5, s1
assert states("jaxtpc_wire")[0] == "submitted"          # no estimate: reserves nothing, kept
assert "over budget" in out and "cancelling edepsim 5 pending task(s) [4-8]" in out, out
assert camp().attempts_used("edepsim", 8) == 0           # cancellation does not use an attempt
print("OK: over budget -> 5 pending stage-1 elements cancelled, no new submissions")

# 4. submit refuses without room
out = dprod("submit", "1")
assert "no room in the storage budget" in out, out
assert count(states("edepsim"), "submitted") == 2

# 5. downstream first: room for 2 tasks with 2A estimated -> 2A gets them before stage 1
finish("jaxtpc_wire", [0], int(0.5 * G))
finish("edepsim", [2, 3], 1 * G)       # 2A task 1 (jobs 2-3) becomes ready
dprod("update-config", config({"limit": "7G", "probe_jobs": 2}))
# used 4G + 0.5G = 4.5G -> room 2.5G: 2A task 1 (0.55G) first, then 1 stage-1 task (1.1G)
out = dprod("advance")
assert states("jaxtpc_wire")[1] == "submitted", states("jaxtpc_wire")
assert count(states("edepsim"), "submitted") == 1, states("edepsim")
print("OK: downstream stage served first")

# 6. min_free: a filesystem with less free space than required stops submissions
dprod("update-config", config({"min_free": "1P"}))
assert "no room in the storage budget" in dprod("submit", "1")
st = dprod("status")
assert "free " in st and "keep 1.0P" in st and "no new submissions" in st, st
dprod("update-config", config({"limit": "1P"}))       # no storage problem: all the rest goes out
dprod("advance")
assert count(states("edepsim"), "new") == 0, states("edepsim")
print("OK: min_free checks the filesystem; a large limit lets everything through")

# 7. config errors are reported
bad = config({"limit": "lots"})
try:
    C.load_campaign(bad)
    raise AssertionError("bad size accepted")
except C.ConfigError as e:
    assert "storage.limit" in str(e), e
print("STORAGE TEST OK")
