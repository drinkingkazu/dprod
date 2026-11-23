"""Stage 3 driver: run pysupera on a task's inputs, and check (or tag) event ids.

    python3 -m doraemon_prod.stages.supera --readout wire [--merged] \\
        --edepsim <edep-sim files> --jaxtpc <JAXTPC task dirs> --outdir <dir> \\
        [-- extra run_pysupera overrides, e.g. distance_threshold=5.0]

--merged (pysupera 4.1+): one run_pysupera for all the task's edep-sim files,
one output supera_<readout>_jFIRST-LAST.h5 (io.input_path and the
reader.jaxtpc_*_path given as lists). pysupera copies each event's identity
(events/job_id, job_event, event_id) from the edep-sim files' /dprod/events;
the driver checks it against the inputs.

Without --merged, as before:

For every edep-sim file (one stage-1 job) this finds the JAXTPC step and hits
(and, for pixel, sensor) files made from it, runs

    run_pysupera reader=jaxtpc_<readout> io.input_path=<edep-sim>
        io.output_path=<outdir>/supera_<readout>_jNNNNNN-NNNNNN.h5
        reader.jaxtpc_seg_path=<step> reader.jaxtpc_inst_path=<hits>
        [reader.jaxtpc_sensor_path=<sensor>] [extra]

and then checks the ids pysupera 4.1 copied (events/job_id, job_event), or,
for outputs without them (pysupera before 4.1, or edep-sim files without
/dprod), adds events/job_id and events/event_id (the event's number within its
job) taken from the edep-sim event table. Either way it checks that pysupera
wrote one output event per input event.

JAXTPC files are paired with jobs through their own ids (see
idreaders.read_jaxtpc): a file may hold one job, or several (JAXTPC
--merge-output).
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
    """{role: {job_id: path}} for the JAXTPC files under the given task dirs
    (a file holding several jobs is the file of each of them)."""
    out = {r: {} for r in roles}
    for d in dirs:
        for role in roles:
            for p in sorted(glob.glob(os.path.join(d, role, "**", "*.h5"), recursive=True)):
                for job in read_jaxtpc(p, os.path.relpath(p, d), {}):
                    if job in out[role]:
                        raise IdReadError("job %d has two %s files: %s, %s" % (
                            job, role, out[role][job], p))
                    out[role][job] = p
    return out


def edepsim_events(path):
    """[(job_id, job_event)] of an edep-sim file, in its event order."""
    with h5py.File(path, "r") as f:
        t = f["event/geant4"][:]
    return [(int(j), int(e)) for j, e in zip(t["run_id"], t["event_id"])]


def tag_ids(path, events):
    """Check the ids pysupera copied into `path`, or add them (see the module doc).
    events: [(job_id, job_event)] of the inputs, in input order."""
    with h5py.File(path, "a") as f:
        n = int(f["n_events"][()])
        if n != len(events):
            raise RuntimeError("%s: pysupera wrote %d events, its inputs have %d" % (
                path, n, len(events)))
        want = np.array(events, dtype=np.int64).reshape(-1, 2)
        if "events/job_event" in f:              # pysupera 4.1: copied from /dprod/events
            got = np.stack([f["events/job_id"][:], f["events/job_event"][:]], axis=1)
            if not np.array_equal(got, want):
                bad = int(np.argmax(np.any(got != want, axis=1)))
                raise RuntimeError("%s: event %d is (job %d, event %d), its input says (%d, %d)"
                                   % (path, bad, got[bad][0], got[bad][1], want[bad][0], want[bad][1]))
            return "checked"
        for name, data in (("job_id", want[:, 0].astype(np.int32)),
                           ("event_id", want[:, 1].astype(np.int32))):
            if "events/" + name in f:
                del f["events/" + name]
            f.create_dataset("events/" + name, data=data)
        return "tagged"


def hydra_list(paths):
    """A Hydra override value listing paths: ['a','b']."""
    return "[" + ",".join("'%s'" % p.replace("\\", "\\\\").replace("'", "\\'") for p in paths) + "]"


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
    ap.add_argument("--merged", action="store_true",
                    help="one pysupera run and output for all inputs (pysupera 4.1+)")
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
    # the pixel reader reads hits as the detected image and needs the sensor file
    roles = ("step", "hits", "sensor") if a.readout == "pixel" else ("step", "hits")
    split = {r: [p for p in a.edepsim if p.endswith("_%s.h5" % r)] for r in ("step", "mctruth")}
    if split["step"]:
        return run_format3(a, exe, split, extra)
    files = index_jaxtpc(a.jaxtpc, roles)
    if a.merged:
        return run_merged(a, exe, roles, files, extra)
    n_done = 0
    for edep in a.edepsim:
        ids = read_event_table(edep, os.path.basename(edep), {})
        if len(ids) != 1:
            raise IdReadError("%s holds %d jobs; expected one" % (edep, len(ids)))
        job, events = next(iter(ids.items()))
        paths = {r: files[r].get(job) for r in roles}
        if not all(paths.values()):
            raise IdReadError("job %d: JAXTPC file missing (%s)" % (
                job, ", ".join("%s=%s" % rp for rp in paths.items())))
        out = os.path.join(a.outdir, "supera_%s_j%0*d-%0*d.h5" % (
            a.readout, JOB_DIGITS, job, JOB_DIGITS, job))
        cmd = exe + ["reader=jaxtpc_%s" % a.readout, "io.input_path=%s" % edep,
               "io.output_path=%s" % out, "reader.jaxtpc_seg_path=%s" % paths["step"],
               "reader.jaxtpc_inst_path=%s" % paths["hits"]]
        if "sensor" in paths:
            cmd.append("reader.jaxtpc_sensor_path=%s" % paths["sensor"])
        cmd += extra
        log("job %d: %s" % (job, " ".join(shlex.quote(c) for c in cmd)))
        t0 = time.time()
        rc = subprocess.call(cmd)
        if rc != 0:
            log("run_pysupera failed for job %d (exit code %d)" % (job, rc))
            return rc
        how = tag_ids(out, [(job, e) for e in events])
        log("job %d: %d events in %.1f s -> %s (ids %s)" % (
            job, len(events), time.time() - t0, os.path.basename(out), how))
        n_done += 1
    log("done: %d job(s)" % n_done)
    return 0


def run_merged(a, exe, roles, files, extra):
    """All edep-sim inputs (in job order) through one run_pysupera, one output."""
    inputs = []
    for edep in a.edepsim:
        events = edepsim_events(edep)
        jobs = sorted({j for j, _ in events})
        if len(jobs) != 1:
            raise IdReadError("%s holds %d jobs; expected one" % (edep, len(jobs)))
        paths = {r: files[r].get(jobs[0]) for r in roles}
        if not all(paths.values()):
            raise IdReadError("job %d: JAXTPC file missing (%s)" % (
                jobs[0], ", ".join("%s=%s" % rp for rp in paths.items())))
        inputs.append((jobs[0], edep, events, paths))
    inputs.sort(key=lambda x: x[0])
    first, last = inputs[0][0], inputs[-1][0]
    out = os.path.join(a.outdir, "supera_%s_j%0*d-%0*d.h5" % (
        a.readout, JOB_DIGITS, first, JOB_DIGITS, last))
    key = {"step": "reader.jaxtpc_seg_path", "hits": "reader.jaxtpc_inst_path",
           "sensor": "reader.jaxtpc_sensor_path"}
    cmd = exe + ["reader=jaxtpc_%s" % a.readout,
                 "io.input_path=%s" % hydra_list([x[1] for x in inputs]),
                 "io.output_path=%s" % out]
    for r in roles:
        paths = [x[3][r] for x in inputs]
        # one merged JAXTPC file for all inputs (--merge-output): name it once
        cmd.append("%s=%s" % (key[r], paths[0] if len(set(paths)) == 1 else hydra_list(paths)))
    cmd += extra
    log("jobs %d-%d (%d inputs): %s" % (first, last, len(inputs),
                                         " ".join(shlex.quote(c) for c in cmd)))
    t0 = time.time()
    rc = subprocess.call(cmd)
    if rc != 0:
        log("run_pysupera failed (exit code %d)" % rc)
        return rc
    events = [e for x in inputs for e in x[2]]
    how = tag_ids(out, events)
    log("done: %d events of %d job(s) in %.1f s -> %s (ids %s)" % (
        len(events), len(inputs), time.time() - t0, os.path.basename(out), how))
    return 0


def run_format3(a, exe, split, extra):
    """Step and mctruth files of all the jobs, with the task's sensor, hits and
    presupera files: one run, one label file."""
    by_job = {}
    for role, paths in split.items():
        for p in paths:
            events = edepsim_events(p)
            jobs = {j for j, _ in events}
            if len(jobs) != 1:
                raise IdReadError("%s holds %d jobs; expected one" % (p, len(jobs)))
            by_job.setdefault(jobs.pop(), {})[role] = (p, events)
    for job, d in by_job.items():
        if set(d) != {"step", "mctruth"} or d["step"][1] != d["mctruth"][1]:
            raise IdReadError("job %d: step and mctruth files do not match" % job)
    jobs = sorted(by_job)
    roles = ("hits", "presupera", "sensor")
    files = index_jaxtpc(a.jaxtpc, roles)
    out = os.path.join(a.outdir, "supera_%s_j%0*d-%0*d.h5" % (
        a.readout, JOB_DIGITS, jobs[0], JOB_DIGITS, jobs[-1]))
    cmd = exe + ["reader=jaxtpc_%s" % a.readout,
                 "io.input_path=%s" % hydra_list([by_job[j]["mctruth"][0] for j in jobs]),
                 "reader.jaxtpc_seg_path=%s" % hydra_list([by_job[j]["step"][0] for j in jobs]),
                 "io.output_path=%s" % out, "io.layout=label"]
    key = {"hits": "reader.jaxtpc_inst_path", "presupera": "reader.jaxtpc_presupera_path",
           "sensor": "reader.jaxtpc_sensor_path"}
    for r in roles:
        if r == "sensor" and a.readout == "wire":
            continue
        paths = [files[r].get(j) for j in jobs]
        if not all(paths):
            raise IdReadError("JAXTPC %s file missing for a job of %s" % (r, jobs))
        cmd.append("%s=%s" % (key[r], paths[0] if len(set(paths)) == 1 else hydra_list(paths)))
    cmd += extra
    log("jobs %d-%d: %s" % (jobs[0], jobs[-1], " ".join(shlex.quote(c) for c in cmd)))
    t0 = time.time()
    rc = subprocess.call(cmd)
    if rc != 0:
        log("run_pysupera failed (exit code %d)" % rc)
        return rc
    how = tag_ids(out, [e for j in jobs for e in by_job[j]["step"][1]])
    log("done in %.1f s -> %s (ids %s)" % (time.time() - t0, os.path.basename(out), how))
    return 0


if __name__ == "__main__":
    sys.exit(main())
