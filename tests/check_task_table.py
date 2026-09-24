"""Check a downstream-stage summary HDF5 (/job table, one row per task).

    python3 tests/check_task_table.py <stage_summary.h5> <n_tasks> <events_per_task>
"""
import sys

import h5py
import numpy as np

path, n_tasks, n_ev = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
with h5py.File(path, "r") as f:
    t = f["job"][:]
    assert int(f.attrs["n_job"]) == len(t) == n_tasks, (len(t), n_tasks)
print(t.dtype.names)
for r in t:
    print(r)
assert (t["task_id"] == np.arange(n_tasks)).all()
assert (t["n_events"] == n_ev).all() and (t["n_input_events"] == n_ev).all()
assert (t["n_input_jobs"] == t["last_job"] - t["first_job"] + 1).all()
assert (t["duration_s"] > 0).all() and (t["avg_rss_mb"] > 0).all()
assert (t["n_gpus"] >= 1).all() and (t["gpu_util_pct"] > 0).all() and (t["gpu_mem_used_mb"] > 0).all()
assert (t["gpu_mem_max_mb"] >= t["gpu_mem_used_mb"]).all()
assert t[t["task_id"] == 1]["attempt"][0] == 2     # task 1 succeeded on its retry
print("TASK TABLE OK")
