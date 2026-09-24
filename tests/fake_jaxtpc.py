"""Stand-in for JAXTPC run_batch.py in local tests.

Mimics the output layout: <outdir>/{sensor,step}/run_<id>/<dataset>_<role>_0000_00.h5
with a `config` group (attr source_file) and one event_NNN group per event.
Like the real run_batch.py --edepsim-ids, a run_id of 0 goes to run_unknown/.
Optionally loads the GPU. Fails once per task if <fail_dir>/fail_<first_job> exists (then removes it).
"""
import argparse
import os
import sys

import h5py

ap = argparse.ArgumentParser()
ap.add_argument("--data", nargs="+", required=True)
ap.add_argument("--outdir", required=True)
ap.add_argument("--dataset", default="sim")
ap.add_argument("--fail-dir")
ap.add_argument("--first-job", type=int)
ap.add_argument("--gpu-seconds", type=float, default=0,
                help="keep the GPU busy this long (JAX matmuls), to test monitoring")
a = ap.parse_args()

if a.gpu_seconds > 0:
    import time
    import jax
    import jax.numpy as jnp
    print("devices:", jax.devices())
    x = jnp.ones((8192, 8192), dtype=jnp.float32)
    t0 = time.time()
    while time.time() - t0 < a.gpu_seconds:
        jnp.dot(x, x).block_until_ready()
    print("GPU load done: %.1f s" % (time.time() - t0))

if a.fail_dir:
    marker = os.path.join(a.fail_dir, "fail_%d" % a.first_job)
    if os.path.exists(marker):
        os.remove(marker)
        print("injected failure for task starting at job %d" % a.first_job)
        sys.exit(3)

for src in a.data:
    with h5py.File(src, "r") as f:
        table = f["event/geant4"][:]
    run_id = int(table[0]["run_id"])
    sub = "run_%010d" % run_id if run_id != 0 else "run_unknown"
    for role in ("sensor", "step"):
        d = os.path.join(a.outdir, role, sub)
        os.makedirs(d, exist_ok=True)
        with h5py.File(os.path.join(d, "%s_%s_0000_00.h5" % (a.dataset, role)), "w") as out:
            out.create_group("config").attrs["source_file"] = os.path.basename(src)
            for i, row in enumerate(table):
                g = out.create_group("event_%03d" % i)
                g.attrs["event_id"] = int(row["event_id"])
                g.attrs["source_event_idx"] = i
    print("processed %s (run %d, %d events)" % (src, run_id, len(table)))
