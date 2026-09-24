"""Stage 3 driver: run pysupera once per stage-1 job, and tag events with ids.

    python3 -m doraemon_prod.stages.supera --readout wire \\
        --edepsim <edep-sim files> --jaxtpc <JAXTPC task dirs> --outdir <dir> \\
        [-- extra run_pysupera overrides, e.g. distance_threshold=5.0]

For every edep-sim file (one stage-1 job) this finds the JAXTPC step and hits
files made from it, runs

    run_pysupera reader=jaxtpc_<readout> io.input_path=<edep-sim>
        io.output_path=<outdir>/supera_<readout>_jNNNNNN-NNNNNN.h5
        reader.jaxtpc_seg_path=<step> reader.jaxtpc_inst_path=<hits> [extra]

and then adds events/job_id and events/event_id (n_events,) to the output,
taken from the edep-sim event table, because pysupera's own format (3.1.0)
does not carry them. It checks that pysupera wrote one output event per input
event before doing so.

JAXTPC files are paired with a job through their config/source_file attribute
(see idreaders.read_jaxtpc), so job 0 (written to run_unknown/) works too.
"""

import argparse
import glob
import os
import shlex
import shutil
import subprocess
import sys
import time

import h5py
import numpy as np

from ..idreaders import IdReadError, read_event_table, read_jaxtpc
from ..layout import JOB_DIGITS


def log(msg):
    print("[supera %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def index_jaxtpc(dirs, roles=("step", "hits")):
    """{role: {job_id: path}} for the JAXTPC files under the given task dirs."""
    out = {r: {} for r in roles}
    for d in dirs:
        for role in roles:
            for p in sorted(glob.glob(os.path.join(d, role, "**", "*.h5"), recursive=True)):
                ids = read_jaxtpc(p, os.path.relpath(p, d), {})
                if len(ids) != 1:
                    raise IdReadError("%s holds %d jobs; expected one" % (p, len(ids)))
                job = next(iter(ids))
                if job in out[role]:
                    raise IdReadError("job %d has two %s files: %s, %s" % (
                        job, role, out[role][job], p))
                out[role][job] = p
    return out


def tag_ids(path, job, event_ids):
    with h5py.File(path, "a") as f:
        n = int(f["n_events"][()])
        if n != len(event_ids):
            raise RuntimeError("%s: pysupera wrote %d events, edep-sim job %d has %d" % (
                path, n, job, len(event_ids)))
        for name, data in (("job_id", np.full(n, job, dtype=np.int32)),
                           ("event_id", np.asarray(event_ids, dtype=np.int32))):
            if "events/" + name in f:
                del f["events/" + name]
            f.create_dataset("events/" + name, data=data)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    extra = []
    if "--" in argv:
        i = argv.index("--")
        argv, extra = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--readout", choices=("wire", "pixel"), required=True)
    ap.add_argument("--edepsim", nargs="+", required=True, help="edep-sim files")
    ap.add_argument("--jaxtpc", nargs="+", required=True, help="JAXTPC task output dirs")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--exe", default=None,
                    help="pysupera command (default: run_pysupera if on PATH, "
                         "else 'python3 -m pysupera._run')")
    a = ap.parse_args(argv)

    os.makedirs(a.outdir, exist_ok=True)
    if a.exe:
        exe = shlex.split(a.exe)
    elif shutil.which("run_pysupera"):
        exe = ["run_pysupera"]
    else:
        exe = [sys.executable, "-m", "pysupera._run"]
    files = index_jaxtpc(a.jaxtpc)
    n_done = 0
    for edep in a.edepsim:
        ids = read_event_table(edep, os.path.basename(edep), {})
        if len(ids) != 1:
            raise IdReadError("%s holds %d jobs; expected one" % (edep, len(ids)))
        job, events = next(iter(ids.items()))
        step, hits = files["step"].get(job), files["hits"].get(job)
        if not step or not hits:
            raise IdReadError("job %d: JAXTPC step/hits file missing (step=%s, hits=%s)" % (
                job, step, hits))
        out = os.path.join(a.outdir, "supera_%s_j%0*d-%0*d.h5" % (
            a.readout, JOB_DIGITS, job, JOB_DIGITS, job))
        cmd = exe + ["reader=jaxtpc_%s" % a.readout, "io.input_path=%s" % edep,
               "io.output_path=%s" % out, "reader.jaxtpc_seg_path=%s" % step,
               "reader.jaxtpc_inst_path=%s" % hits] + extra
        log("job %d: %s" % (job, " ".join(shlex.quote(c) for c in cmd)))
        t0 = time.time()
        rc = subprocess.call(cmd)
        if rc != 0:
            log("run_pysupera failed for job %d (exit code %d)" % (job, rc))
            return rc
        tag_ids(out, job, events)
        log("job %d: %d events in %.1f s -> %s" % (job, len(events), time.time() - t0,
                                                   os.path.basename(out)))
        n_done += 1
    log("done: %d job(s)" % n_done)
    return 0


if __name__ == "__main__":
    sys.exit(main())
