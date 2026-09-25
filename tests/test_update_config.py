"""Test `dprod update-config` on a running campaign (the S3DF smoke-campaign case:
stage 3 was disabled at init and is enabled later).

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_update_config.py <scratch dir>
"""
import os
import shutil
import sys

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
os.environ["DPROD_STATE"] = ""           # do not remember campaigns in ~/.config
os.environ["DPROD_BATCH"] = "1"          # no confirmation prompts in tests
site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1))
base = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
base = base.replace("campaign: localtest_doraemon_v0.0", "campaign: updtest").replace(
    "n_jobs: 6", "n_jobs: 2").replace("events_per_job: 3", "events_per_job: 2").replace(
    " --gpu-seconds 8", "").replace("monitor_gpu: true", "monitor_gpu: false")
v1 = os.path.join(ROOT, "v1.yaml")
open(v1, "w").write(base)                                  # 3A: enabled: false, command "true"
TAG = "updtest"


def dprod(*args, rc=0):
    print("+ dprod " + " ".join(args))
    got = cli.main(["--site", site, "--campaign", TAG] + list(args))
    assert got == rc, (got, rc)


dprod("init", v1)
dprod("advance")                                            # 1
dprod("advance")                                            # 2A (planned once 1 is done; 3A disabled)
c = Campaign(C.load_site(site), TAG)
c.sync()
assert [t["status"] for t in D.tasks(c.con, "jaxtpc_wire")] == ["done"]
dprod("submit", "3A", rc=1)                                 # disabled

# unsafe change is refused, nothing written
bad = os.path.join(ROOT, "bad.yaml")
open(bad, "w").write(base.replace("events_per_job: 2", "events_per_job: 5").replace("merge: 2", "merge: 1"))
dprod("update-config", bad, rc=1)
assert open(os.path.join(c.dir, "campaign.yaml")).read() == base
print("OK: events_per_job / merge changes refused after stages ran")

# a moved input_dir with identical files is accepted; with a changed file it is refused
import shutil as _sh
moved = os.path.join(ROOT, "moved_inputs")
_sh.copytree(os.path.join(HERE, "..", "configs", "edepsim"), moved)
mv = os.path.join(ROOT, "moved.yaml")
open(mv, "w").write(base.replace("input_dir: configs/edepsim", "input_dir: %s" % moved))
dprod("update-config", mv, "--dry-run")
with open(os.path.join(moved, "mpvmpr.yaml"), "a") as f:
    f.write("# changed\n")
dprod("update-config", mv, "--dry-run", rc=1)
print("OK: input_dir move accepted only when the stage-1 files are identical")

# the real update: enable 3A with a working command
v2 = os.path.join(ROOT, "v2.yaml")
new3a = '''  supera_wire:
    alias: 3A
    parent: jaxtpc_wire
    merge: 1
    profile: cpu
    also_inputs: [edepsim]
    command: "python3 {test_dir}/fake_supera.py --edepsim {inputs_edepsim} --outdir {outdir}"
    outputs: "*.h5"
    id_reader: pysupera
'''
i = base.index("  supera_wire:")
open(v2, "w").write(base[:i] + new3a)
dprod("update-config", v2, "--dry-run")
assert open(os.path.join(c.dir, "campaign.yaml")).read() == base
dprod("update-config", v2)
backups = [f for f in os.listdir(c.dir) if f.startswith("campaign.yaml.")]
assert len(backups) == 1 and open(os.path.join(c.dir, backups[0])).read() == base
dprod("advance")
c = Campaign(C.load_site(site), TAG)
c.sync()
t3 = D.tasks(c.con, "supera_wire")
assert [t["status"] for t in t3] == ["done"], [dict(t) for t in t3]
print("OK: 3A enabled by update-config and ran")

# provenance: both config versions, each task block names its own
f3 = [os.path.join(c.dir, f["path"]) for f in D.task_files(c.con, "supera_wire", 0)]
p = read_provenance(f3[0])
assert p["stages"] == ["edepsim", "jaxtpc_wire", "supera_wire"], p["stages"]
cur_sha = p["campaign_config"]["sha256"]
hist = p["campaign_config_history"]
assert len(hist) == 1
old_sha = list(hist.values())[0]["sha256"]
assert cur_sha != old_sha
assert task_blocks(p, "supera_wire")[0]["campaign_config_sha256"] == cur_sha
assert {b["campaign_config_sha256"] for b in task_blocks(p, "edepsim")} == {old_sha}
assert list(hist.values())[0]["config"]["stages"]["supera_wire"]["enabled"] is False
print("OK: provenance keeps both campaign-config versions; each task block names its version")
print("UPDATE-CONFIG TEST OK")
