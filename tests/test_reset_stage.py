"""Test `dprod reset-stage`: outputs of the stage and of the stages below it are
unregistered and deleted, tasks go back to 'new' with a fresh max_attempts count,
active elements are cancelled (and waited for), upstream stages are untouched, and
campaigns inheriting the stages are protected.

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_reset_stage.py <scratch dir>

Jobs never run: the test "finishes" tasks by writing output files and registering
them, as the sync of a successful job would.
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
from doraemon_prod import cli, config as C, db as D, layout as L  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(ROOT)
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""
os.environ["DPROD_BATCH"] = "1"


class PendingScheduler(LocalScheduler):
    stubborn = False          # True: cancelled elements keep RUNNING (a job that will not die)

    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        json.dump({str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None,
                            "start": None, "end": None, "node": None} for i in range(n_tasks)},
                  open(self._state_path(jid), "w"))
        return jid

    def cancel(self, array_job_id, indices=None):
        p = self._state_path(array_job_id)
        states = json.load(open(p))
        for i in (indices if indices is not None else [int(k) for k in states]):
            states[str(i)]["state"] = "RUNNING" if PendingScheduler.stubborn else "CANCELLED"
        json.dump(states, open(p, "w"))


K.get_scheduler = lambda site, cdir: PendingScheduler(site, os.path.join(cdir, "local_sched"))

site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, ROOT), 1))
TAG = "resettest"
cfg = os.path.join(ROOT, "campaign.yaml")
open(cfg, "w").write(open(os.path.join(HERE, "campaign_local_test.yaml")).read().replace(
    "campaign: localtest_doraemon_v0.0", "campaign: " + TAG))
CDIR = os.path.join(ROOT, "storage", TAG)


def dprod(*args, rc=0):
    print("+ dprod " + " ".join(args))
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        got = cli.main(["--site", site, "--campaign", TAG] + list(args))
    print(out.getvalue() + err.getvalue(), end="")
    assert got == rc, (got, rc)
    return out.getvalue() + err.getvalue()


def camp():
    return K.Campaign(C.load_site(site), TAG)


def states(stage):
    return {t["task_id"]: t["status"] for t in D.tasks(camp().con, stage)}


def finish(stage, task_ids):
    c = camp()
    with c.con:
        for tid in task_ids:
            t = c.con.execute("SELECT * FROM tasks WHERE stage = ? AND task_id = ?", (stage, tid)).fetchone()
            name = L.task_name(stage, t["first_job"], t["last_job"])
            rel = "%s/%s/out.h5" % (L.data_rel_dir(stage, t["first_job"]), name)
            os.makedirs(os.path.dirname(os.path.join(CDIR, rel)), exist_ok=True)
            open(os.path.join(CDIR, rel), "w").write("x" * 1000)
            c.con.execute("UPDATE attempts SET state = 'done', end_time = ? WHERE stage = ? AND task_id = ?"
                          " AND attempt = ?", (time.time(), stage, tid, t["n_attempts"]))
            events = {str(j): [0, 1] for j in range(t["first_job"], t["last_job"] + 1)}
            D.replace_task_outputs(c.con, stage, tid, [{"path": rel, "role": "hits", "size": 1000,
                                                        "events": events}])
            D.set_task_status(c.con, stage, tid, D.DONE, n_events=2 * len(events))


def write_records(stage, task_ids):
    """The job-side records a finished attempt leaves (for the monitoring page)."""
    from doraemon_prod import webdata as W
    c = camp()
    for tid in task_ids:
        t = c.con.execute("SELECT * FROM tasks WHERE stage = ? AND task_id = ?", (stage, tid)).fetchone()
        W.write_record(CDIR, stage, {"task_id": tid, "attempt": t["n_attempts"], "first_job": t["first_job"],
                                     "last_job": t["last_job"],
                                     "name": L.task_name(stage, t["first_job"], t["last_job"])},
                       "done", {"n_events": 2, "wall_s": 1.0})


def page_done(stage):
    """Done counts of a stage in both views of the monitoring page."""
    web = os.path.join(CDIR, "web")
    return [[s for s in json.load(open(os.path.join(web, f)))["stages"] if s["name"] == stage][0]
            ["counts"]["done"] for f in ("status.json", "jobs.json")]


def n_files(stage):
    return camp().con.execute("SELECT COUNT(*) FROM files WHERE stage = ?", (stage,)).fetchone()[0]


dprod("init", cfg)
dprod("submit", "1")
finish("edepsim", range(6))
dprod("submit", "2A")
finish("jaxtpc_wire", [0, 1])                 # task 2 stays queued
write_records("jaxtpc_wire", [0, 1])
leftover = os.path.join(CDIR, "data", "jaxtpc_wire", "j000000", "stray_from_an_old_version.h5")
open(leftover, "w").write("old")
open(camp().merged_summary_path("jaxtpc_wire"), "w").write("summary")
assert n_files("jaxtpc_wire") == 2 and states("jaxtpc_wire")[2] == "submitted"

# 1. dry run: shows the plan, changes nothing
out = dprod("reset-stage", "2A", "--dry-run")
assert "jaxtpc_wire [2A], supera_wire [3A]" in out and "3 task(s) back to 'new'" in out, out
assert "1 queued/running" in out and os.path.exists(leftover) and n_files("jaxtpc_wire") == 2

# 2. no terminal and no --confirm: refused; wrong confirmation: refused
dprod("reset-stage", "2A", rc=1)
dprod("reset-stage", "2A", "--confirm", "2B", rc=1)
assert n_files("jaxtpc_wire") == 2

# 3. a derived campaign inherits 2A: refused without --allow-inherited
other = os.path.join(ROOT, "storage", "derivedtest")
os.makedirs(other)
open(os.path.join(other, "campaign.yaml"), "w").write(
    "campaign: derivedtest\nstages:\n  jaxtpc_wire:\n    external: %s\n" % TAG)
out = dprod("reset-stage", "2A", "--confirm", "2A", rc=1)
assert "derivedtest" in out and "--allow-inherited" in out, out
shutil.rmtree(other)

# 4. a job that does not stop: nothing is deleted
PendingScheduler.stubborn = True
c = camp()
try:
    c.reset_stage("jaxtpc_wire", wait_s=0, out=lambda m: None)
    raise AssertionError("deleted while a job was still running")
except K.CampaignError as e:
    assert "still not stopped" in str(e), e
assert os.path.exists(leftover) and n_files("jaxtpc_wire") == 2
out = []
try:                    # a second try waits for it too, although it is 'cancelled' by now
    camp().reset_stage("jaxtpc_wire", wait_s=0, out=out.append)
    raise AssertionError("second try deleted while a job was still running")
except K.CampaignError as e:
    assert "still not stopped" in str(e) and not any("cancelling" in m for m in out), (e, out)
PendingScheduler.stubborn = False
sched = os.path.join(CDIR, "local_sched")
for fn in os.listdir(sched):                     # slurm finally kills it
    p = os.path.join(sched, fn)
    if fn.endswith(".json"):
        st = json.load(open(p))
        json.dump({k: dict(v, state="CANCELLED" if v["state"] == "RUNNING" else v["state"])
                   for k, v in st.items()}, open(p, "w"))
print("OK: dry run, confirmation, inherited stages and running jobs protect the outputs")

# 5. the reset: 2A (and 3A below it) cleared, stage 1 untouched
dprod("submit", "2A")                         # task 2 went back to 'new' in step 4: queue it again
out = dprod("reset-stage", "2A", "--confirm", "2A")
assert not os.path.exists(os.path.join(CDIR, "data", "jaxtpc_wire"))
assert not os.path.exists(camp().merged_summary_path("jaxtpc_wire"))
assert n_files("jaxtpc_wire") == 0 and set(states("jaxtpc_wire").values()) == {"new"}, states("jaxtpc_wire")
c = camp()
assert c.con.execute("SELECT COUNT(*) FROM events e JOIN files f ON f.id = e.file_id"
                     " WHERE f.stage = 'jaxtpc_wire'").fetchone()[0] == 0
assert all(c.attempts_used("jaxtpc_wire", t) == 0 for t in range(3))     # fresh max_attempts count
assert set(states("edepsim").values()) == {"done"} and n_files("edepsim") == 6
assert os.path.exists(os.path.join(CDIR, "data", "edepsim"))
assert dprod("lookup", "0", "0").count("jaxtpc_wire") == 0
assert page_done("jaxtpc_wire") == [0, 0], page_done("jaxtpc_wire")     # old records are history
assert page_done("edepsim")[0] == 6
print("OK: 2A outputs unregistered and deleted, tasks new, stage 1 untouched")

# 6. it runs again: new attempts, inputs from stage 1
dprod("submit", "2A")
assert set(states("jaxtpc_wire").values()) == {"submitted"}
finish("jaxtpc_wire", [0, 1, 2])
assert n_files("jaxtpc_wire") == 3
print("OK: the reset stage is submitted and finishes again")

# 7. resetting the root resets everything below it
out = dprod("reset-stage", "1", "--confirm", "1")
assert set(states("edepsim").values()) == {"new"} and n_files("edepsim") == 0 and n_files("jaxtpc_wire") == 0
assert not os.path.exists(os.path.join(CDIR, "data", "edepsim"))
print("RESET-STAGE TEST OK")
