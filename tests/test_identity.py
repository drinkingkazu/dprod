"""Test event identity: /dprod written by the worker, the id readers for JAXTPC
format 2 (entry_NNNNNN, merged files) and pysupera 4.1, the stage-3 driver's id
check, `dprod audit` and `dprod lookup --event-id`.

  apptainer exec -B /home/kazu,/tmp <test.sif or larcv2 image> python3 tests/test_identity.py <scratch dir>

No job runs: files are written by hand in the formats the applications write
(see JAXTPC production/entries.py, pysupera pysupera/identity.py).
"""
import contextlib
import io
import os
import shutil
import sys

import h5py
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import campaign as K  # noqa: E402
from doraemon_prod import cli, config as C, db as D, identity as I, layout as L  # noqa: E402
from doraemon_prod.idreaders import IdReadError, read_jaxtpc, read_pysupera  # noqa: E402
from doraemon_prod.stages import supera  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(ROOT)
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""
os.environ["DPROD_BATCH"] = "1"
EPJ = 3

assert I.event_id(4, 2, EPJ) == 14 and I.split_event_id(14, EPJ) == (4, 2)


def edepsim_file(path, job, n=EPJ, m=None, task=None):
    with h5py.File(path, "w") as f:
        t = np.zeros(n, dtype=[("run_id", "<i4"), ("event_id", "<i4")])
        t["run_id"], t["event_id"] = job, np.arange(n)
        f["event/geant4"] = t
    if m is not None:
        I.write(path, m, task, "edepsim", list(zip(t["run_id"], t["event_id"])), order="file")


def manifest(stage="edepsim", alias="1"):
    return {"campaign": "idtest", "stage": stage, "stage_config": {"alias": alias},
            "identity": {"origin_campaign": "idtest", "events_per_job": EPJ, "root_stage": "edepsim"}}


# 1. /dprod written in file order; a manifest without "identity" writes nothing
p = os.path.join(ROOT, "e5.h5")
edepsim_file(p, 5, m=manifest(), task={"task_id": 5, "attempt": 1, "first_job": 5, "last_job": 5})
d = I.read(p)
assert d["campaign"] == "idtest" and d["order"] == "file" and d["n_events"] == EPJ, d
assert d["events"] == {"job_id": [5] * 3, "job_event": [0, 1, 2], "event_id": [15, 16, 17]}, d
assert I.write(p, {"campaign": "x", "stage": "s", "stage_config": {}}, None, "r", []) is False
print("OK: stage-1 identity in file order")

# 2. JAXTPC format 2, merged: two jobs in one file, ids per entry
def jaxtpc_file(path, sources, entries):
    with h5py.File(path, "w") as f:
        c = f.create_group("config")
        c.attrs["format_version"] = 2
        c.create_dataset("source_files", data=np.array(sources, dtype=h5py.string_dtype()))
        for i, attrs in enumerate(entries):
            g = f.create_group("entry_%06d" % i)
            for k, v in attrs.items():
                g.attrs[k] = v


m = os.path.join(ROOT, "hits_merged.h5")
jaxtpc_file(m, ["/x/edepsim_j000006-000006.h5", "/x/edepsim_j000007-000007.h5"],
            [dict(source_index=k, source_event_idx=e, job_id=6 + k, job_event=e, event_id=(6 + k) * EPJ + e)
             for k in (0, 1) for e in range(EPJ)])
assert read_jaxtpc(m, "hits_merged.h5", {}) == {6: [0, 1, 2], 7: [0, 1, 2]}
# without dprod ids (inputs without /dprod): job from the input's name, event from event_id
n = os.path.join(ROOT, "hits_plain.h5")
jaxtpc_file(n, ["/x/edepsim_j000006-000006.h5", "/x/edepsim_j000007-000007.h5"],
            [dict(source_index=k, source_event_idx=e, event_id=e) for k in (0, 1) for e in range(2)])
assert read_jaxtpc(n, "hits_plain.h5", {}) == {6: [0, 1], 7: [0, 1]}
bad = os.path.join(ROOT, "hits_bad.h5")
jaxtpc_file(bad, ["/x/somefile.h5"], [dict(source_index=0, source_event_idx=0, event_id=0)])
try:
    read_jaxtpc(bad, "hits_bad.h5", {})
    raise AssertionError("an entry without a job was accepted")
except IdReadError as e:
    assert "does not name one job" in str(e), e
print("OK: JAXTPC format-2 ids, merged and plain")

# 3. pysupera 4.1 (job_event) vs older tagged outputs (event_id = event within the job)
def supera_file(path, cols):
    with h5py.File(path, "w") as f:
        f["n_events"] = np.int64(len(next(iter(cols.values()))))
        for k, v in cols.items():
            f["events/" + k] = np.asarray(v)


s41 = os.path.join(ROOT, "s41.h5")
supera_file(s41, {"job_id": [6, 6, 7], "job_event": [0, 1, 0], "event_id": [18, 19, 21]})
assert read_pysupera(s41, "s41.h5", {}) == {6: [0, 1], 7: [0]}
s40 = os.path.join(ROOT, "s40.h5")
supera_file(s40, {"job_id": [6, 6], "event_id": [0, 1]})
assert read_pysupera(s40, "s40.h5", {}) == {6: [0, 1]}
# the driver checks what pysupera copied, and tags what it did not
assert supera.tag_ids(s41, [(6, 0), (6, 1), (7, 0)]) == "checked"
try:
    supera.tag_ids(s41, [(6, 0), (7, 0), (6, 1)])
    raise AssertionError("wrong ids accepted")
except RuntimeError as e:
    assert "event 1 is (job 6, event 1)" in str(e), e
plain = os.path.join(ROOT, "s_plain.h5")
supera_file(plain, {"x": [0, 0, 0]})
assert supera.tag_ids(plain, [(6, 0), (6, 1), (7, 0)]) == "tagged"
assert read_pysupera(plain, "s_plain.h5", {}) == {6: [0, 1], 7: [0]}
assert supera.hydra_list(["/a/b.h5", "/c/it's.h5"]) == "['/a/b.h5','/c/it\\'s.h5']"
print("OK: pysupera ids read, checked and tagged")

# 4. audit and lookup --event-id against a campaign's bookkeeping
site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, ROOT), 1))
TAG = "idtest"
cfg = os.path.join(ROOT, "campaign.yaml")
open(cfg, "w").write(open(os.path.join(HERE, "campaign_local_test.yaml")).read().replace(
    "campaign: localtest_doraemon_v0.0", "campaign: " + TAG))


def dprod(*args, rc=0):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        got = cli.main(["--site", site, "--campaign", TAG] + list(args))
    print("+ dprod %s\n%s" % (" ".join(args), out.getvalue()), end="")
    assert got == rc, (got, rc)
    return out.getvalue()


dprod("init", cfg)
c = K.Campaign(C.load_site(site), TAG)
mf = c._manifest(c.cfg["stages"]["edepsim"], 0, [])
assert mf["identity"] == {"origin_campaign": TAG, "events_per_job": 3, "root_stage": "edepsim"}, mf
dprod("submit", "1", "--limit", "2")
paths = []
with c.con:
    for job in (0, 1):
        t = {"task_id": job, "attempt": 1, "first_job": job, "last_job": job}
        rel = "%s/%s.h5" % (L.data_rel_dir("edepsim", job), L.task_name("edepsim", job, job))
        path = os.path.join(c.dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        edepsim_file(path, job, m=mf, task=t)
        D.replace_task_outputs(c.con, "edepsim", job, [{"path": rel, "role": "edepsim", "size": 1,
                                                         "events": {str(job): [0, 1, 2]}}])
        D.set_task_status(c.con, "edepsim", job, D.DONE, n_events=3)
        paths.append(path)
out = dprod("audit", paths[1])
assert "campaign:    idtest" in out and "event_id 3-5" in out and "3 events match" in out, out
assert "edepsim [1], role edepsim; task 1 attempt 1, jobs 1-1" in out, out
dprod("audit", p, rc=1)                         # not registered in idtest
assert "NOT registered" in dprod("audit", p, rc=1)
with c.con:                                      # the bookkeeping disagrees with the file
    fid = c.con.execute("SELECT id FROM files WHERE path LIKE ?", ("%000001-000001.h5",)).fetchone()[0]
    c.con.execute("DELETE FROM events WHERE file_id = ? AND event_id = 2", (fid,))
assert "DIFFER" in dprod("audit", paths[1], rc=1)
assert "no /dprod identity" in dprod("audit", m, rc=1)
out = dprod("lookup", "--event-id", "4")
assert "= job 1 event 1" in out and "edepsim_j000001-000001.h5" in out, out
dprod("lookup", "--event-id", "4", "1", "1", rc=1)
print("IDENTITY TEST OK")
