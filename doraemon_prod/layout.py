"""Naming conventions, directory layout and seed derivation.

Shared by the controller and the worker, so this module is stdlib-only.

Identifiers
-----------
* job id   : 0-indexed integer, one per stage-1 (edep-sim) task. It is written
             into the edep-sim output as the Geant4 run_id and carried through
             every downstream stage.
* event id : 0-indexed Geant4 event number within a job.
* task     : one unit of work at a given stage (= one slurm array element).
             Every task covers a consecutive, inclusive job range
             [first_job, last_job]. Stage-1 tasks cover exactly one job and
             task_id == job_id.

Campaign directory (on shared storage)
--------------------------------------
    <storage_root>/<campaign>/
        campaign.yaml          frozen campaign configuration
        bookkeeping.sqlite     written only by the controller
        code/                  snapshot of this package used by the workers
        inputs/<stage>/        snapshot of stage input files (macros, yaml...)
        submissions/<stage>/   array manifests (JSON) and sbatch scripts
        summaries/<stage>/     one JSON per task attempt, written by workers, plus
                               (stage 1) a job summary HDF5 per attempt
        <campaign>_<stage>_summary.h5   job summaries merged over done jobs
        logs/<stage>/          tarball of each attempt's work-dir logs
        data/<stage>/jNNNNNN/  output data, sharded by 1000 jobs
"""

import hashlib
import re

JOB_DIGITS = 6
SHARD_SIZE = 1000

_TASK_NAME_RE = re.compile(r"_j(\d+)-(\d+)")


def task_name(stage, first_job, last_job):
    """e.g. edepsim_j000120-000120, jaxtpc_wire_j000000-000039"""
    return "%s_j%0*d-%0*d" % (stage, JOB_DIGITS, first_job, JOB_DIGITS, last_job)


def parse_job_range(name):
    """Return (first_job, last_job) encoded in a file/dir name, or None."""
    m = _TASK_NAME_RE.search(name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def shard_dir(first_job):
    return "j%0*d" % (JOB_DIGITS, (first_job // SHARD_SIZE) * SHARD_SIZE)


def data_rel_dir(stage, first_job):
    return "data/%s/%s" % (stage, shard_dir(first_job))


def summary_rel_path(stage, name, attempt):
    return "summaries/%s/%s_a%02d.json" % (stage, name, attempt)


def job_summary_rel_path(stage, name, attempt):
    return "summaries/%s/%s_a%02d.h5" % (stage, name, attempt)


def logs_rel_path(stage, name, attempt):
    return "logs/%s/%s_a%02d.tgz" % (stage, name, attempt)


def derive_seed(master_seed, *keys, bits=31):
    """Deterministic, well-mixed seed in [1, 2**bits) from a master seed + keys.

    The same (master_seed, keys) always gives the same seed, so any job can be
    reproduced exactly; different keys give statistically independent seeds.
    """
    text = ":".join(str(k) for k in (master_seed,) + keys)
    h = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
    return (h % ((1 << bits) - 1)) + 1
