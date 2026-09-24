"""Cross-check a merged job summary HDF5 against the edep-sim files it came from.

    python3 tests/check_summary.py <merged.h5> <edepsim data dir>
"""
import glob
import os
import sys

import h5py
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from doraemon_prod.layout import parse_job_range
from doraemon_prod.summary import PDG_COUNTS

merged, data_dir = sys.argv[1:3]
with h5py.File(merged, "r") as f:
    n = {k: int(f.attrs["n_" + k]) for k in ("job", "event", "particle")}
    job, ev, pa = f["job"][:n["job"]], f["event"][:n["event"]], f["particle"][:n["particle"]]
print("merged: %d jobs, %d events, %d primaries" % (len(job), len(ev), len(pa)))

srcs = {parse_job_range(os.path.basename(p))[0]: p
        for p in glob.glob(os.path.join(data_dir, "*", "*.h5"))}
assert sorted(job["job_id"]) == sorted(srcs), (sorted(job["job_id"]), sorted(srcs))
for j in job:
    e = ev[j["event_start"]:j["event_end"]]
    assert (e["job_id"] == j["job_id"]).all() and len(e) == j["n_events"]
    with h5py.File(srcs[int(j["job_id"])], "r") as s:
        g4 = s["event/geant4"][:]
        assert (e["event_id"] == g4["event_id"]).all()
        assert (e["num_primaries"] == g4["num_primaries"]).all()
        assert (e["num_segments"] == g4["num_steps"]).all()
        for k, row in enumerate(e):
            p = pa[row["particle_start"]:row["particle_end"]]
            prim = s["primary/geant4"][k]
            vtx = s["vertex/geant4"][k]
            assert len(p) == row["num_primaries"] == len(prim)
            assert (p["event_id"] == row["event_id"]).all() and (p["job_id"] == j["job_id"]).all()
            assert (p["pdg"] == prim["pdg"]).all() and (p["track_id"] == prim["track_id"]).all()
            assert np.allclose(p["E"], prim["ke"] + prim["mass"])
            assert np.isclose(row["primary_ke_sum"], prim["ke"].sum(), rtol=1e-5)
            for name, codes in PDG_COUNTS:
                assert row[name] == np.isin(prim["pdg"], codes).sum(), name
            assert (p["interaction_id"] == prim["interaction_id"]).all()
            byint = {int(v): i for i, v in enumerate(vtx["interaction_id"])}
            for c in ("x", "y", "z", "t"):
                assert np.allclose(p[c], vtx[c][[byint[int(v)] for v in p["interaction_id"]]])
    assert j["duration_s"] > 0 and j["max_rss_mb"] > 0 and j["node"]
    assert 0 < j["avg_rss_mb"] <= j["max_rss_mb"], (j["avg_rss_mb"], j["max_rss_mb"])
    assert np.isnan(j["gpu_util_pct"]) and np.isnan(j["gpu_mem_used_mb"])   # CPU job
print("job:", job.dtype.names)
print(job[:2])
print("event:", ev.dtype.names)
print(ev[:2])
print("particle:", pa[:2])
print("SUMMARY OK")
