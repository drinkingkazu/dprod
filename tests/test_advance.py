"""Test `dprod advance` / `watch`: streaming across stages, throttling, locking.

Run inside the stage-1 image (edep-sim + h5py):
  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_advance.py <scratch dir>

Uses a deferred scheduler: submitted array elements stay PENDING until the test
releases them (they then really run, like LocalScheduler), so we can check that
stage 2A starts while stage-1 jobs are still queued.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import campaign as K  # noqa: E402
from doraemon_prod import cli, db as D  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(os.path.join(ROOT, "fail"))
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""           # do not remember campaigns in ~/.config
os.environ["DPROD_BATCH"] = "1"          # no confirmation prompts in tests


class DeferredScheduler(LocalScheduler):
    pending = []          # shared across Campaign instances: (jid, script, idx)
    auto = False          # run everything pending on each query (for watch)

    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        states = {str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None,
                           "start": None, "end": None, "node": None} for i in range(n_tasks)}
        json.dump(states, open(self._state_path(jid), "w"))
        DeferredScheduler.pending += [(jid, script, i) for i in range(n_tasks)]
        return jid

    def run(self, k=None):
        todo = DeferredScheduler.pending[:k] if k else list(DeferredScheduler.pending)
        DeferredScheduler.pending = DeferredScheduler.pending[len(todo):]
        for jid, script, idx in todo:
            env = dict(os.environ, SLURM_ARRAY_JOB_ID=jid, SLURM_ARRAY_TASK_ID=str(idx),
                       SLURM_JOB_ID="%s%03d" % (jid, idx))
            t0 = time.time()
            with open(os.path.join(self.state_dir, "%s_%d.out" % (jid, idx)), "w") as f:
                rc = subprocess.call(["bash", script], stdout=f, stderr=subprocess.STDOUT, env=env)
            p = self._state_path(jid)
            states = json.load(open(p))
            states[str(idx)] = {"state": "COMPLETED" if rc == 0 else "FAILED",
                                "exit_code": "%d:0" % rc, "elapsed_s": time.time() - t0,
                                "start": t0, "end": time.time(), "node": "test"}
            json.dump(states, open(p, "w"))
        return len(todo)

    def query(self, ids):
        if DeferredScheduler.auto:
            self.run()
        return super().query(ids)


K.get_scheduler = lambda site, cdir: DeferredScheduler(site, os.path.join(cdir, "local_sched"))

site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
text = text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1)
open(site, "w").write(text)
camp = os.path.join(ROOT, "campaign.yaml")
ctext = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
ctext = ctext.replace("campaign: localtest_doraemon_v0.0", "campaign: advancetest")
ctext = ctext.replace("events_per_job: 3", "events_per_job: 2")
ctext = ctext.replace(" --gpu-seconds 8", "")
ctext = ctext.replace("monitor_gpu: true", "monitor_gpu: false")
open(camp, "w").write(ctext)

TAG = "advancetest"


def dprod(*args):
    print("+ dprod " + " ".join(args))
    rc = cli.main(["--site", site, "--campaign", TAG] + list(args))
    assert rc == 0, rc


def open_c():
    from doraemon_prod import config as C
    return K.Campaign(C.load_site(site), TAG)


def states(stage):
    c = open_c()
    c.sync()
    return {t["task_id"]: t["status"] for t in D.tasks(c.con, stage)}


sched = DeferredScheduler(None, os.path.join(ROOT, "storage", TAG, "local_sched"))
dprod("init", camp)

# 1. throttle: at most 4 stage-1 elements queued; nothing downstream is ready yet
dprod("advance", "--max-queued", "4")
s1 = states("edepsim")
assert [s1[i] for i in range(6)] == ["submitted"] * 4 + ["new"] * 2, s1
assert set(states("jaxtpc_wire").values()) == {"new"}

# 2. jobs 0 and 1 finish -> 2A task 0 (jobs 0-1) goes out while jobs 2-5 are not done
assert sched.run(2) == 2
dprod("advance", "--max-queued", "4")
s1, s2 = states("edepsim"), states("jaxtpc_wire")
assert [s1[i] for i in range(6)] == ["done", "done", "submitted", "submitted", "submitted", "submitted"], s1
assert s2 == {0: "submitted", 1: "new", 2: "new"}, s2
print("OK: 2A task 0 submitted while stage-1 jobs 2-5 are still queued")

# 3. a stage-2 failure is retried by advance --recover
open(os.path.join(ROOT, "fail", "fail_2"), "w").close()
sched.run()                                   # jobs 2-5 + 2A task 0
dprod("advance")                              # 2A tasks 1, 2
sched.run()                                   # task 1 fails once, task 2 ok
assert states("jaxtpc_wire") == {0: "done", 1: "failed", 2: "done"}
dprod("advance")                              # without --recover: nothing
assert states("jaxtpc_wire")[1] == "failed"
dprod("advance", "--recover")
sched.run()
assert states("jaxtpc_wire") == {0: "done", 1: "done", 2: "done"}
print("OK: failed 2A task recovered by advance --recover")

# 4. lock: a second controller (another process) waits / times out while one holds it
holder = subprocess.Popen([sys.executable, "-c", """
import sys, time; sys.path.insert(0, %r)
from doraemon_prod import config as C, campaign as K
c = K.Campaign(C.load_site(%r), %r)
with c.lock():
    print("locked", flush=True); time.sleep(8)
""" % (os.path.join(HERE, ".."), site, TAG)], stdout=subprocess.PIPE, universal_newlines=True)
assert holder.stdout.readline().strip() == "locked"
t0 = time.time()
try:
    with open_c().lock(timeout=3, out=lambda m: None):
        raise AssertionError("second lock acquired")
except K.CampaignError as e:
    assert "locked by another dprod" in str(e) and time.time() - t0 >= 3
holder.wait()
with open_c().lock(timeout=3):
    pass                                      # free again once the holder exits
print("OK: a concurrent controller process is blocked by the campaign lock")

# 5. watch on a fresh extension: runs to completion by itself, then stops
dprod("extend", "8")
DeferredScheduler.auto = True
dprod("watch", "--interval", "1", "--recover", "--merge-summary")
s1, s2 = states("edepsim"), states("jaxtpc_wire")
assert set(s1.values()) == {"done"} and len(s1) == 8, s1
assert set(s2.values()) == {"done"} and len(s2) == 4, s2
assert os.path.exists(os.path.join(ROOT, "storage", TAG, "%s_jaxtpc_wire_summary.h5" % TAG))
print("OK: watch submitted jobs 6-7 and 2A task 3, then stopped by itself")
print("ADVANCE TEST OK")
