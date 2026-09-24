"""Check /provenance in the outputs of the local test campaign.

    python3 tests/check_provenance.py <campaign dir> <scratch dir>
"""
import glob
import json
import os
import sys

import h5py
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from doraemon_prod import provenance as P

cdir, scratch = sys.argv[1:3]
inputs = os.path.join(cdir, "inputs", "edepsim")
ctext = open(os.path.join(cdir, "campaign.yaml")).read()
raw = yaml.safe_load(ctext)


def check_edepsim_block(prov, job):
    blocks = P.task_blocks(prov, "edepsim")
    b = [x for x in blocks if x["first_job"] == job]
    assert len(b) == 1, [x["first_job"] for x in blocks]
    b = b[0]
    summ = json.load(open(sorted(glob.glob(os.path.join(
        cdir, "summaries/edepsim/edepsim_j%06d-%06d_a*.json" % (job, job))))[-1]))
    assert b["seeds"] == summ["seeds"], (b["seeds"], summ["seeds"])
    assert b["files"]["geometry"] == open(os.path.join(inputs, "BigLArCube.gdml")).read()
    assert "/edep/runId %d" % job in b["files"]["macro"]
    assert "/edep/random/randomSeed %d" % b["seeds"]["geant4"] in b["files"]["macro"]
    gen = b["config"]["generator_config"]
    assert gen["SEED"] == b["seeds"]["generator"]
    assert gen == yaml.safe_load(b["files"]["generator_config"]), "yaml tree round trip"
    assert len(b["software"]["edep-sim"]["sha256"]) == 64
    assert len(b["software"]["edep-sim_src"]["git_commit"]) == 40
    return b


def check_common(prov, stages):
    assert prov["stages"] == stages, prov["stages"]
    assert prov["format_version"] == 2 and prov["campaign"] == raw["campaign"]
    assert prov["campaign_config"]["campaign.yaml"] == ctext
    assert prov["campaign_config"]["config"] == raw
    for st in stages:
        # stage_config holds only the options set in the campaign config
        assert prov[st]["stage_config"] == raw["stages"][st], st


# ---- stage 1: every edep-sim file
files = sorted(glob.glob(os.path.join(cdir, "data/edepsim/*/*.h5")))
assert files
for p in files:
    prov = P.read_provenance(p)
    check_common(prov, ["edepsim"])
    job = P.task_blocks(prov, "edepsim")[0]["first_job"]
    check_edepsim_block(prov, job)
print("stage 1: %d file(s) OK" % len(files))

# ---- stage 2A: sensor files carry edepsim (their source job) + jaxtpc_wire blocks
sensor = sorted(glob.glob(os.path.join(cdir, "data/jaxtpc_wire/*/*/sensor/*/*.h5")))
step = sorted(glob.glob(os.path.join(cdir, "data/jaxtpc_wire/*/*/step/*/*.h5")))
assert sensor and step
for p in step:
    assert P.read_provenance(p) is None, p
det = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       "fake_detector.yaml")).read())
for p in sensor:
    prov = P.read_provenance(p)
    check_common(prov, ["edepsim", "jaxtpc_wire"])
    with h5py.File(p) as f:     # the edep-sim job this sensor file was made from
        src_job = int(f["config"].attrs["source_file"].split("_j")[1][:6])
    (e,) = P.task_blocks(prov, "edepsim")            # exactly its own source job
    assert e["first_job"] == src_job
    check_edepsim_block(prov, src_job)
    (w,) = P.task_blocks(prov, "jaxtpc_wire")
    assert w["first_job"] <= src_job <= w["last_job"] and "--gpu-seconds" in w["command"]
    assert w["config"]["detector_config"] == det
    assert "sha256" in w["software"]["fake_jaxtpc"]
print("stage 2A: %d sensor file(s) OK, %d step file(s) without provenance" % (
    len(sensor), len(step)))

# ---- simulated stage 3 merging two stage-2 tasks (4 jobs) into one file
out = os.path.join(scratch, "fake_stage3.h5")
with h5py.File(out, "w") as f:
    f.create_dataset("data", data=[1, 2, 3])
m = {"campaign": raw["campaign"], "stage": "supera_wire", "campaign_dir": cdir,
     "stage_config": {"alias": "3A", "handler": "command"},
     "stage_config_raw": raw["stages"]["supera_wire"], "image": "test.sif"}
t = {"task_id": 0, "attempt": 1, "first_job": 0, "last_job": 3, "seeds": {"task": 1}}
up = [p for p in sensor if "j000000-000001" in p or "j000002-000003" in p]
assert len(up) == 4
P.write(out, P.collect(m, t, command="supera ...", inputs=up), up)
prov = P.read_provenance(out)
check_common(prov, ["edepsim", "jaxtpc_wire", "supera_wire"])
assert [b["first_job"] for b in P.task_blocks(prov, "edepsim")] == [0, 1, 2, 3]
assert [(b["first_job"], b["last_job"]) for b in P.task_blocks(prov, "jaxtpc_wire")] == [(0, 1), (2, 3)]
for j in range(4):
    check_edepsim_block(prov, j)
with h5py.File(out) as f:
    n_blobs = len(f["provenance/_blobs"])
    # the geometry text of the 4 jobs is a single stored object (hard links)
    addrs = {hash(f["provenance/edepsim/%s/files/geometry" % n])   # same object -> same hash
             for n in f["provenance/edepsim"] if n.startswith("j")}
    assert len(addrs) == 1, addrs
print("stage 3 (simulated): stages %s, %d edepsim blocks, %d jaxtpc_wire blocks, %d unique texts"
      % (prov["stages"], len(P.task_blocks(prov, "edepsim")),
         len(P.task_blocks(prov, "jaxtpc_wire")), n_blobs))
print("PROVENANCE OK")
