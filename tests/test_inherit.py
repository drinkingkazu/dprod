"""Test derived campaigns: stages inherited (read-only) from another campaign.

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_inherit.py <scratch dir>
"""
import io
import os
import shutil
import sys
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import cli, db as D, config as C  # noqa: E402
from doraemon_prod.campaign import Campaign  # noqa: E402
from doraemon_prod.provenance import read_provenance, task_blocks  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(os.path.join(ROOT, "fail"))
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""
os.environ["DPROD_BATCH"] = "1"
site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1))
base = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
src_cfg = os.path.join(ROOT, "src.yaml")
open(src_cfg, "w").write(base.replace("campaign: localtest_doraemon_v0.0", "campaign: srcamp").replace(
    "n_jobs: 6", "n_jobs: 4").replace("events_per_job: 3", "events_per_job: 2").replace(
    " --gpu-seconds 8", "").replace("monitor_gpu: true", "monitor_gpu: false"))
der_cfg = os.path.join(ROOT, "derived.yaml")
open(der_cfg, "w").write('''campaign: derived
description: re-run stage 3 on the stage-2 output of srcamp
seed: 777
max_attempts: 2
inherit:
  campaign: srcamp
  stages: [jaxtpc_wire]
stages:
  supera_wire:
    alias: 3A
    parent: jaxtpc_wire
    merge: 1
    profile: cpu
    also_inputs: [edepsim]
    command: "python3 {test_dir}/fake_supera.py --edepsim {inputs_edepsim} --outdir {outdir}"
    outputs: "*.h5"
    id_reader: pysupera
''')


def d(tag, *args, rc=0):
    print("+ dprod --campaign %s %s" % (tag, " ".join(args)))
    buf = io.StringIO()
    with redirect_stdout(buf):
        got = cli.main(["--site", site, "--campaign", tag] + list(args))
    print(buf.getvalue(), end="")
    assert got == rc, (got, rc)
    return buf.getvalue()


def camp(tag):
    c = Campaign(C.load_site(site), tag)
    c.sync()
    return c


# source: jobs 0-1 through stage 1 and 2A (2A task 0 = jobs 0-1); jobs 2-3 later
assert cli.main(["--site", site, "init", src_cfg]) == 0
d("srcamp", "submit", "1", "--tasks", "0-1")
d("srcamp", "advance", "--stages", "2A")
s = camp("srcamp")
assert [t["status"] for t in D.tasks(s.con, "jaxtpc_wire")] == ["done", "new"]

# derived campaign
assert cli.main(["--site", site, "init", der_cfg]) == 0
c = camp("derived")
assert list(c.cfg["stages"]) == ["edepsim", "jaxtpc_wire", "supera_wire"], list(c.cfg["stages"])
assert c.cfg["stages"]["edepsim"]["external"] == "srcamp" and c.cfg["stages"]["jaxtpc_wire"]["external"] == "srcamp"
assert [t["status"] for t in D.tasks(c.con, "edepsim")] == ["done", "done", "new", "new"]
assert len(D.tasks(c.con, "supera_wire")) == 2
out = d("derived", "status", "--no-sync")
assert "edepsim[1]*" in out and "inherited from campaign srcamp" in out
print("OK: derived campaign imports the source's stages (read-only) and defines its own 3A tasks")

d("derived", "submit", "1", rc=1)                       # read-only
d("derived", "extend", "10", rc=1)
d("derived", "mark", "2A", "0", "--abandon", rc=1)
print("OK: inherited stages cannot be submitted, extended or marked here")

d("derived", "advance")                                  # 3A task 0 (jobs 0-1)
c = camp("derived")
assert [t["status"] for t in D.tasks(c.con, "supera_wire")] == ["done", "new"]
f3 = [os.path.join(c.dir, f["path"]) for f in D.task_files(c.con, "supera_wire", 0)]
p = read_provenance(f3[0])
assert p["stages"] == ["edepsim", "jaxtpc_wire", "supera_wire"], p["stages"]
assert task_blocks(p, "supera_wire")[0]["seeds"]["task"] != 0
assert p["campaign_config"]["config"]["campaign"] == "derived"
assert [v["config"]["campaign"] for v in p["campaign_config_history"].values()] == ["srcamp"]
out = d("derived", "lookup", "1", "0")
src_dir = os.path.join(ROOT, "storage", "srcamp")
assert out.count(src_dir) >= 2 and os.path.join(ROOT, "storage", "derived") in out, out
print("OK: 3A ran on the source's files; provenance chains to the generator with both configs; lookup spans both")

# source continues with jobs 2-3: the derived campaign follows by itself
d("srcamp", "submit", "1")
d("srcamp", "advance")
d("srcamp", "sync")                                      # the source's own bookkeeping catches up
d("derived", "advance")
c = camp("derived")
assert [t["status"] for t in D.tasks(c.con, "supera_wire")] == ["done", "done"]
assert [t["status"] for t in D.tasks(c.con, "edepsim")] == ["done"] * 4
print("OK: new source output is imported at sync; the remaining 3A task ran")

d("derived", "merge-summary")
d("derived", "web", "--no-sync")
keep = [os.path.join(src_dir, f["path"]) for f in D.task_files(camp("srcamp").con, "jaxtpc_wire", 0)]
d("derived", "destroy", "--confirm", "derived")
assert not os.path.exists(os.path.join(ROOT, "storage", "derived"))
assert all(os.path.exists(x) for x in keep) and os.path.exists(os.path.join(src_dir, "bookkeeping.sqlite"))
out = d("srcamp", "status", "--no-sync")
print("OK: destroying the derived campaign leaves the source intact")
print("INHERIT TEST OK")
