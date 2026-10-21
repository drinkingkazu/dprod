"""Test the worker's event validation: every output role must hold exactly the
task's input events (a stage that silently drops one fails), unless the stage
sets allow_missing_events.

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_events.py <scratch dir>
"""
import contextlib
import io
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import config as C, db as D, layout as L  # noqa: E402
from doraemon_prod import cli  # noqa: E402
from doraemon_prod.campaign import Campaign  # noqa: E402
from doraemon_prod.worker import TaskFailure, check_events  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
FAIL = os.path.join(ROOT, "fail")
os.makedirs(FAIL)
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""
os.environ["DPROD_BATCH"] = "1"

# 1. the expected-event sets and the check itself
pairs = [(4, e) for e in range(200)] + [(5, e) for e in range(200) if e != 7]
packed = L.pack_events(pairs)
assert packed == {"4": [[0, 199]], "5": [[0, 6], [8, 199]]}, packed
assert L.unpack_events(packed) == set(pairs)
task = {"expected": L.pack_events([(1, 0), (1, 1), (1, 2), (2, 0)])}
full = {(1, 0), (1, 1), (1, 2), (2, 0)}
assert check_events({"sensor": full, "hits": set(full)}, task, False) == []
for allow in (False, True):          # an event that is not an input: always a failure
    try:
        check_events({"sensor": full | {(3, 0)}}, task, allow)
        raise AssertionError("an extra event was accepted")
    except TaskFailure as e:
        assert "not among the task's inputs: (job 3, event 0)" in str(e), e
try:
    check_events({"sensor": full, "hits": full - {(1, 2)}}, task, False)
    raise AssertionError("a missing event was accepted")
except TaskFailure as e:
    assert "role hits is missing 1 of 4 input event(s): (job 1, event 2)" in str(e), e
    assert "allow_missing_events: true" in str(e), e
w = check_events({"sensor": full, "hits": full - {(1, 2)}}, task, True)
assert w == ["role hits is missing 1 of 4 input event(s): (job 1, event 2)"], w
# manifests from before the event sets: counts only
assert check_events({"s": full}, {"expected_events": 4}, False) == []
try:
    check_events({"s": {(1, 0)}}, {"expected_events": 4}, False)
    raise AssertionError
except TaskFailure as e:
    assert "missing 3 of 4" in str(e), e
print("OK: check_events")

# 2. a real (fake) stage 2 that drops an event
site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, FAIL), 1))
base = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
base = base.replace(" --gpu-seconds 8", "").replace("monitor_gpu: true", "monitor_gpu: false")
base = base.replace("n_jobs: 6", "n_jobs: 2")


def write_cfg(tag, extra=""):
    p = os.path.join(ROOT, tag + ".yaml")
    t = base.replace("campaign: localtest_doraemon_v0.0", "campaign: " + tag)
    if extra:
        t = t.replace("    id_reader: jaxtpc\n  supera_wire:", "    id_reader: jaxtpc\n" + extra + "  supera_wire:")
    open(p, "w").write(t)
    return p


def d(tag, *args, rc=0):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        got = cli.main(["--site", site, "--campaign", tag] + list(args))
    print("+ dprod %s\n%s" % (" ".join(args), buf.getvalue()), end="")
    assert got == rc, (got, rc)
    return buf.getvalue()


def camp(tag):
    c = Campaign(C.load_site(site), tag)
    c.sync()
    return c


assert cli.main(["--site", site, "init", write_cfg("evstrict")]) == 0
d("evstrict", "submit", "1")
open(os.path.join(FAIL, "skip_0"), "w").close()
d("evstrict", "submit", "2A")
c = camp("evstrict")
(t,) = D.tasks(c.con, "jaxtpc_wire")
assert t["status"] == D.FAILED, dict(t)
out = d("evstrict", "failures", "2A", "-v")
assert "role sensor is missing 1 of 6 input event(s): (job 0, event 2)" in out, out
assert "skipping it" in out and "allow_missing_events: true" in out, out
assert not D.task_files(c.con, "jaxtpc_wire", 0)            # nothing registered
d("evstrict", "recover", "2A")                              # the marker is gone: all events
assert D.tasks(camp("evstrict").con, "jaxtpc_wire")[0]["status"] == D.DONE
print("OK: a dropped event fails the task (with the log's reason); the retry passes")

cfg = write_cfg("evallow", "    allow_missing_events: true\n")
assert cli.main(["--site", site, "init", cfg]) == 0
d("evallow", "submit", "1")
open(os.path.join(FAIL, "skip_0"), "w").close()
d("evallow", "submit", "2A")
c = camp("evallow")
(t,) = D.tasks(c.con, "jaxtpc_wire")
assert t["status"] == D.DONE and t["n_events"] == 5, dict(t)
ids = sorted((r["job_id"], r["event_id"]) for r in c.con.execute(
    "SELECT DISTINCT e.job_id, e.event_id FROM events e JOIN files f ON f.id = e.file_id"
    " WHERE f.stage = 'jaxtpc_wire'"))
assert ids == [(0, 0), (0, 1), (1, 0), (1, 1), (1, 2)], ids
out = d("evallow", "tasks", "2A")
print("OK: with allow_missing_events the task is done, with a warning")
print("EVENTS TEST OK")
