"""Test re-routing queued jobs: cancel --queued + resubmit with slurm overrides, and move.

Run inside the stage-1 image (edep-sim + h5py):
  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_requeue.py <scratch dir>
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
from doraemon_prod import scheduler as SCH  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402
from doraemon_prod.webdata import from_records  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(os.path.join(ROOT, "fail"))
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""           # do not remember campaigns in ~/.config
os.environ["DPROD_BATCH"] = "1"          # no confirmation prompts in tests


class Deferred(LocalScheduler):
    """Elements stay PENDING until run(); records cancel/update calls."""
    pending, calls = [], []

    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        json.dump({str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None, "start": None,
                            "end": None, "node": None} for i in range(n_tasks)},
                  open(self._state_path(jid), "w"))
        Deferred.pending += [(jid, script, i) for i in range(n_tasks)]
        return jid

    def _set(self, jid, idx, **info):
        p = self._state_path(jid)
        st = json.load(open(p))
        st[str(idx)].update(info)
        json.dump(st, open(p, "w"))

    def run(self, k=None):
        todo = Deferred.pending[:k] if k else list(Deferred.pending)
        Deferred.pending = Deferred.pending[len(todo):]
        for jid, script, idx in todo:
            env = dict(os.environ, SLURM_ARRAY_JOB_ID=jid, SLURM_ARRAY_TASK_ID=str(idx),
                       SLURM_JOB_ID="%s%03d" % (jid, idx))
            with open(os.path.join(self.state_dir, "%s_%d.out" % (jid, idx)), "w") as f:
                rc = subprocess.call(["bash", script], stdout=f, stderr=subprocess.STDOUT, env=env)
            self._set(jid, idx, state="COMPLETED" if rc == 0 else "FAILED", end=time.time(),
                      start=time.time(), elapsed_s=1.0, node="test")
        return len(todo)

    def cancel(self, jid, indices=None):
        Deferred.calls.append(("cancel", jid, indices))
        for e in [e for e in Deferred.pending if e[0] == jid and (indices is None or e[2] in indices)]:
            Deferred.pending.remove(e)
            self._set(jid, e[2], state="CANCELLED")

    def update(self, jid, indices, fields):
        Deferred.calls.append(("update", jid, indices, dict(fields)))


K.get_scheduler = lambda site, cdir: Deferred(site, os.path.join(cdir, "local_sched"))

site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
text = text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1)
open(site, "w").write(text)
camp = os.path.join(ROOT, "campaign.yaml")
ctext = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
ctext = ctext.replace("campaign: localtest_doraemon_v0.0", "campaign: requeuetest")
ctext = ctext.replace("events_per_job: 3", "events_per_job: 2")
ctext = ctext.replace(" --gpu-seconds 8", "").replace("monitor_gpu: true", "monitor_gpu: false")
open(camp, "w").write(ctext)
TAG = "requeuetest"
CDIR = os.path.join(ROOT, "storage", TAG)


def dprod(*args):
    print("+ dprod " + " ".join(args))
    assert cli.main(["--site", site, "--campaign", TAG] + list(args)) == 0


def camp_():
    from doraemon_prod import config as C
    c = K.Campaign(C.load_site(site), TAG)
    c.sync()
    return c


sched = Deferred(None, os.path.join(CDIR, "local_sched"))
dprod("init", camp)

# 1. submit 4 stage-1 jobs, one runs, 3 stay queued
dprod("submit", "1", "--limit", "4")
sched.run(1)
c = camp_()
assert {t["task_id"]: t["status"] for t in D.tasks(c.con, "edepsim")} == \
    {0: "done", 1: "submitted", 2: "submitted", 3: "submitted", 4: "new", 5: "new"}

# 2. cancel only the queued ones -> back to 'new', attempts not used
dprod("cancel", "1", "--queued")
c = camp_()
st = {t["task_id"]: t["status"] for t in D.tasks(c.con, "edepsim")}
assert st == {0: "done", 1: "new", 2: "new", 3: "new", 4: "new", 5: "new"}, st
assert [c.attempts_used("edepsim", i) for i in (1, 2, 3)] == [0, 0, 0]
assert {r["state"] for r in c.con.execute("SELECT state FROM attempts WHERE stage='edepsim' AND task_id IN (1,2,3)")} == {"cancelled"}
assert not Deferred.pending
print("OK: cancel --queued returned tasks 1-3 to 'new' without using an attempt")

# 3. job-side monitoring view ignores the cancelled attempts
v = from_records(CDIR)
assert v["stages"][0]["counts"]["submitted"] == 0 and v["stages"][0]["counts"]["new"] == 5, v["stages"][0]["counts"]
print("OK: job-records view does not count cancelled elements as queued")

# 4. resubmit everything with a different partition/account (site config untouched)
dprod("submit", "1", "--partition", "roma", "--account", "neutrino:other", "--slurm", "mem=4G")
subs = sorted(os.listdir(os.path.join(CDIR, "submissions", "edepsim")))
script = open(os.path.join(CDIR, "submissions", "edepsim", [x for x in subs if x.endswith(".sh")][-1])).read()
assert "#SBATCH --partition=roma" in script and "#SBATCH --account=neutrino:other" in script \
    and "#SBATCH --mem=4G" in script, script
mans = [json.load(open(os.path.join(CDIR, "submissions", "edepsim", x))) for x in subs if x.endswith(".json")]
mans = [m for m in mans if m.get("slurm_override")]          # this resubmission (arrays of <= 4)
assert [(t["task_id"], t["attempt"]) for m in mans for t in m["tasks"]] == [(1, 2), (2, 2), (3, 2), (4, 1), (5, 1)]
assert all(m["slurm_override"]["partition"] == "roma" for m in mans)
sched.run()
c = camp_()
assert all(t["status"] == "done" for t in D.tasks(c.con, "edepsim"))
assert [c.attempts_used("edepsim", i) for i in range(6)] == [1] * 6
print("OK: resubmitted with --partition/--account override; attempt numbers continue, 1 attempt used each")

# 5. move queued elements in place (scontrol update); whole array vs single elements
dprod("extend", "9")
dprod("submit", "1")                          # jobs 6-8 queued in one array
Deferred.calls[:] = []
dprod("move", "1", "--partition", "milano", "--account", "mli:nu-ml-dev")
assert Deferred.calls[0][0] == "update" and Deferred.calls[0][2] is None, Deferred.calls
assert Deferred.calls[0][3] == {"Partition": "milano", "Account": "mli:nu-ml-dev"}
Deferred.calls[:] = []
dprod("move", "1", "--tasks", "7", "--time", "01:00:00")
assert Deferred.calls[0][2] == [1] and Deferred.calls[0][3] == {"TimeLimit": "01:00:00"}, Deferred.calls
c = camp_()
assert all(t["status"] == "submitted" for t in D.tasks(c.con, "edepsim", task_ids=[6, 7, 8]))
assert [c.attempts_used("edepsim", i) for i in (6, 7, 8)] == [1, 1, 1]
print("OK: move updates the whole array when all its queued elements are selected, else single elements")
try:                                          # only partition/account/qos/time are movable
    camp_().move("edepsim", {"gpus": "1"})
    raise AssertionError("move accepted a non-movable option")
except K.CampaignError as e:
    assert "cancel --queued" in str(e)

# 6. jobs cancelled outside dprod (plain scancel): not counted, task back to 'new', recover resubmits
dprod("cancel", "1", "--tasks", "6")             # (via dprod, for comparison)
for jid, script, idx in list(Deferred.pending):  # 7, 8: "scancel" by hand
    sched.cancel(jid, [idx])
c = camp_()
st = {t["task_id"]: t["status"] for t in D.tasks(c.con, "edepsim", task_ids=[6, 7, 8])}
assert st == {6: "new", 7: "new", 8: "new"}, st
assert [c.attempts_used("edepsim", i) for i in (6, 7, 8)] == [0, 0, 0]
dprod("recover", "1")                            # picks up cancelled tasks, too
c = camp_()
assert [t["n_attempts"] for t in D.tasks(c.con, "edepsim", task_ids=[6, 7, 8])] == [2, 2, 2]
sched.run()
print("OK: external scancel is not counted as a failure; recover resubmits cancelled tasks")

# 7. databases from before this fix: failures that were really cancellations get corrected
c = camp_()
with c.con:
    c.con.execute("UPDATE attempts SET state = 'failed', sched_state = 'CANCELLED', reason ="
                  " 'scheduler state CANCELLED (exit 0:15), no worker summary'"
                  " WHERE stage = 'edepsim' AND task_id = 8 AND attempt = 2")
    c.con.execute("UPDATE tasks SET status = 'failed' WHERE stage = 'edepsim' AND task_id = 8")
c = camp_()                                     # sync runs the fix
t8 = D.tasks(c.con, "edepsim", task_ids=[8])[0]
assert t8["status"] == "new" and c.attempts_used("edepsim", 8) == 0, dict(t8)
print("OK: legacy 'failed' cancellations become cancelled; task 8 back to new")

# 8. reset-attempts: a task that really used up max_attempts (2 here) can be retried again
with c.con:
    for att in (3, 4):
        c.con.execute("INSERT INTO attempts (stage, task_id, attempt, submission_id, array_index, state,"
                      " reason) VALUES ('edepsim', 8, ?, 1, 0, 'failed', 'command exit code 1')", (att,))
    c.con.execute("UPDATE tasks SET status = 'failed', n_attempts = 4 WHERE stage = 'edepsim' AND task_id = 8")
c = camp_()
assert c.candidates("edepsim", recovery=True, task_ids=[8]) == []
dprod("reset-attempts", "1", "--tasks", "8")
c = camp_()
assert c.attempts_used("edepsim", 8) == 0 and len(c.candidates("edepsim", recovery=True, task_ids=[8])) == 1
assert c.con.execute("SELECT COUNT(*) FROM attempts WHERE stage='edepsim' AND task_id=8").fetchone()[0] == 4
print("OK: reset-attempts makes an exhausted task retryable again, history kept")

# 9. the real scontrol / scancel command lines
cmds = []
SCH._run = lambda cmd: cmds.append(cmd) or ""
SCH.SlurmScheduler({}).update("4471", None, {"Partition": "roma", "Account": "x"})
SCH.SlurmScheduler({}).update("4471", [3, 5], {"QOS": "normal"})
SCH.SlurmScheduler({}).cancel("4471", [3, 5])
assert cmds == [["scontrol", "update", "JobId=4471", "Partition=roma", "Account=x"],
                ["scontrol", "update", "JobId=4471_3", "QOS=normal"],
                ["scontrol", "update", "JobId=4471_5", "QOS=normal"],
                ["scancel", "4471_3", "4471_5"]], cmds
print("OK: scontrol/scancel command lines")
print("REQUEUE TEST OK")
