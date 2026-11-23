"""Test the step/mctruth edep-sim files: the pysupera command the stage-3 driver
builds from them, and the job summary.

  python3 tests/test_format3.py <scratch dir>
"""
import os
import shutil
import sys

import h5py
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod.idreaders import IdReadError  # noqa: E402
from doraemon_prod.stages import supera  # noqa: E402
from doraemon_prod.summary import extract_edepsim  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
shutil.rmtree(ROOT, ignore_errors=True)
PRIM = [(c, "<f4") for c in ("ke", "mass", "px", "py", "pz")] + \
       [(c, "<i4") for c in ("interaction_id", "track_id", "pdg")]
VERT = [(c, "<f4") for c in "xyzt"] + [("interaction_id", "<i4")]


def edepsim(job, role, n=2):
    p = os.path.join(ROOT, "edepsim_j%06d-%06d_%s.h5" % (job, job, role))
    with h5py.File(p, "w") as f:
        t = np.zeros(n, dtype=[(c, "<i4") for c in (
            "run_id", "event_id", "num_vertices", "num_primaries", "num_particles",
            "num_steps")])
        t["run_id"], t["event_id"], t["num_primaries"] = job, np.arange(n), 1
        f["event/geant4"] = t
        f.create_group("schema").attrs["role"] = role
        for i in range(n if role == "mctruth" else 0):
            f["entry_%06d/primary" % i] = np.array([(1, 2, 0, 0, 1, 7, 0, 13)], dtype=PRIM)
            f["entry_%06d/vertex" % i] = np.array([(3, 4, 5, 6, 7)], dtype=VERT)
    return p


for role in ("sensor", "hits", "presupera"):
    os.makedirs(os.path.join(ROOT, "jax", role))
    with h5py.File(os.path.join(ROOT, "jax", role, "x.h5"), "w") as f:
        f.create_group("config").attrs["format_version"] = 3
        f["config"]["source_files"] = np.array(["a", "b"], dtype=h5py.string_dtype())
        for i in range(4):
            f.create_group("entry_%06d" % i).attrs.update(dict(
                source_index=i // 2, source_event_idx=i % 2, job_id=4 + i // 2,
                job_event=i % 2, event_id=8 + i))
files = [edepsim(j, r) for j in (4, 5) for r in ("mctruth", "step")]

# a pysupera that records its command and writes an empty output of 4 events
fake = os.path.join(ROOT, "fake.py")
open(fake, "w").write("""import sys, h5py, numpy as np
d = dict(x.split("=", 1) for x in sys.argv[1:])
open(d["io.output_path"] + ".cmd", "w").write("\\n".join(sys.argv[1:]))
h5py.File(d["io.output_path"], "w")["n_events"] = np.int64(4)
""")


def run(readout, inputs):
    return supera.main(["--readout", readout, "--edepsim"] + inputs + [
        "--jaxtpc", os.path.join(ROOT, "jax"), "--outdir", os.path.join(ROOT, "out"),
        "--exe", "%s %s" % (sys.executable, fake)])


for readout in ("wire", "pixel"):
    assert run(readout, files) == 0
    out = os.path.join(ROOT, "out", "supera_%s_j000004-000005.h5" % readout)
    cmd = open(out + ".cmd").read()
    assert "io.layout=label" in cmd
    assert "io.input_path=" + supera.hydra_list([files[0], files[2]]) in cmd, cmd
    assert "reader.jaxtpc_seg_path=" + supera.hydra_list([files[1], files[3]]) in cmd, cmd
    assert "reader.jaxtpc_presupera_path=" + os.path.join(ROOT, "jax", "presupera", "x.h5") in cmd, cmd
    assert ("jaxtpc_sensor_path" in cmd) == (readout == "pixel")
    with h5py.File(out, "r") as f:
        assert f["events/job_id"][:].tolist() == [4, 4, 5, 5]
try:
    run("wire", files[:3])                       # job 5 without its step file
    raise AssertionError("a missing step file was accepted")
except IdReadError as e:
    assert "job 5" in str(e), e
print("OK: pysupera command for step/mctruth inputs")

events, particles = extract_edepsim(files[0])
assert particles["E"].tolist() == [3, 3] and particles["x"].tolist() == [3, 3], particles
print("OK: summary from an mctruth file")
