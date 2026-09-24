"""Stand-in for doraemon_prod.stages.supera in tests: one output per edep-sim file,
tagged with events/job_id and events/event_id like the real driver."""
import argparse
import os

import h5py
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--edepsim", nargs="+", required=True)
ap.add_argument("--outdir", required=True)
a = ap.parse_args()
os.makedirs(a.outdir, exist_ok=True)
for src in a.edepsim:
    ev = h5py.File(src, "r")["event/geant4"][:]
    job = int(ev["run_id"][0])
    with h5py.File(os.path.join(a.outdir, "supera_wire_j%06d-%06d.h5" % (job, job)), "w") as f:
        f["n_events"] = len(ev)
        f["events/job_id"] = ev["run_id"].astype(np.int32)
        f["events/event_id"] = ev["event_id"].astype(np.int32)
    print("job", job, len(ev), "events")
