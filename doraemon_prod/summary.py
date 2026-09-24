"""Job summary HDF5: per-job files written by the worker, merged across jobs.

Stage 1 (edep-sim): /job, /event, /particle tables, described below.
Downstream stages (JAXTPC, ...): a /job table only, one row per task
(see TASK_DTYPE), rebuilt from the workers' JSON summaries on every merge.

Needs h5py + numpy, so it runs inside a container (worker side, or via
`dprod merge-summary`, which runs `python3 -m doraemon_prod.summary merge`).

Layout (same for the per-job file and the merged file)
------------------------------------------------------
/job       one row per job
    job_id, attempt, n_events, start_time (unix s), duration_s, node,
    max_rss_mb, avg_rss_mb, gpu_util_pct, gpu_mem_used_mb  (GPU columns NaN
    for CPU jobs; averages are time averages over the job), seed_geant4, seed_generator,
    event_start, event_end     -> fence post into /event rows [start, end)
/event     one row per event
    job_id, event_id, particle_start, particle_end (fence post into /particle),
    num_vertices, num_primaries, num_particles (all Geant4 particles),
    n_proton, n_pion_charged, n_kaon_charged, n_pion0, n_neutron,
    n_electron, n_positron, n_muon, n_antimuon, n_photon   (primaries only),
    primary_ke_sum (MeV), num_segments (Geant4 steps in sensitive volumes)
/particle  flat array of primary particles
    job_id, event_id, interaction_id, track_id, pdg,
    x, y, z (mm), t (ns)   creation point = vertex of the particle's interaction
    E, px, py, pz (MeV)    E = ke + mass

In a per-job file the fence posts are local; in the merged file they index the
merged datasets. The merged file's attrs n_job/n_event/n_particle are the
committed row counts: rows beyond them (from an interrupted merge) are ignored
and truncated by the next merge.
"""

import argparse
import os
import sys

import h5py
import numpy as np

VERSION = 2

JOB_DTYPE = np.dtype([
    ("job_id", "i4"), ("attempt", "i2"), ("n_events", "i4"),
    ("start_time", "f8"), ("duration_s", "f4"), ("node", "S64"), ("max_rss_mb", "f4"),
    ("avg_rss_mb", "f4"), ("gpu_util_pct", "f4"), ("gpu_mem_used_mb", "f4"),
    ("seed_geant4", "i8"), ("seed_generator", "i8"),
    ("event_start", "i8"), ("event_end", "i8"),
])

# (column, pdg codes) counted among the primaries of each event
PDG_COUNTS = (
    ("n_proton", (2212,)),
    ("n_pion_charged", (211, -211)),
    ("n_kaon_charged", (321, -321)),
    ("n_pion0", (111,)),
    ("n_neutron", (2112,)),
    ("n_electron", (11,)),
    ("n_positron", (-11,)),
    ("n_muon", (13,)),
    ("n_antimuon", (-13,)),
    ("n_photon", (22,)),
)

EVENT_DTYPE = np.dtype(
    [("job_id", "i4"), ("event_id", "i4"), ("particle_start", "i8"), ("particle_end", "i8"),
     ("num_vertices", "i4"), ("num_primaries", "i4"), ("num_particles", "i4")]
    + [(name, "i2") for name, _ in PDG_COUNTS]
    + [("primary_ke_sum", "f4"), ("num_segments", "i8")])

PARTICLE_DTYPE = np.dtype([
    ("job_id", "i4"), ("event_id", "i4"), ("interaction_id", "i4"), ("track_id", "i4"),
    ("pdg", "i4"), ("x", "f4"), ("y", "f4"), ("z", "f4"), ("t", "f4"),
    ("E", "f4"), ("px", "f4"), ("py", "f4"), ("pz", "f4"),
])

TABLES = (("job", JOB_DTYPE), ("event", EVENT_DTYPE), ("particle", PARTICLE_DTYPE))
FENCES = {"job": ("event_start", "event_end", "event"),
          "event": ("particle_start", "particle_end", "particle")}


class SummaryError(Exception):
    pass


# --------------------------------------------------------------------------- extraction

def extract_edepsim(path):
    """Event and primary-particle tables from an edep-sim HDF5 file (local fence posts)."""
    with h5py.File(path, "r") as f:
        ev = f["event/geant4"][:]
        prim = f["primary/geant4"]
        vert = f["vertex/geant4"]
        events = np.zeros(len(ev), EVENT_DTYPE)
        chunks = []
        start = 0
        for i, row in enumerate(ev):
            p = prim[i]
            pp = np.zeros(len(p), PARTICLE_DTYPE)
            pp["job_id"] = row["run_id"]
            pp["event_id"] = row["event_id"]
            pp["interaction_id"] = p["interaction_id"]
            pp["track_id"] = p["track_id"]
            pp["pdg"] = p["pdg"]
            pp["E"] = p["ke"] + p["mass"]
            for c in ("px", "py", "pz"):
                pp[c] = p[c]
            # 4-position: the creation point is shared by all primaries of an
            # interaction and stored once per interaction in the vertex table
            vtx = vert[i]
            vrow = {int(v): k for k, v in enumerate(vtx["interaction_id"])}
            if len(vrow) != len(vtx):
                raise SummaryError("event %d: duplicate interaction_id in vertex table"
                                   % row["event_id"])
            try:
                rows = np.array([vrow[int(k)] for k in p["interaction_id"]], dtype=np.int64)
            except KeyError as e:
                raise SummaryError("event %d: primary with interaction_id %s has no vertex"
                                   % (row["event_id"], e))
            for c in ("x", "y", "z", "t"):
                pp[c] = vtx[c][rows]
            chunks.append(pp)

            e = events[i]
            e["job_id"], e["event_id"] = row["run_id"], row["event_id"]
            e["particle_start"], e["particle_end"] = start, start + len(p)
            e["num_vertices"] = row["num_vertices"]
            e["num_primaries"] = row["num_primaries"]
            e["num_particles"] = row["num_particles"]
            for name, codes in PDG_COUNTS:
                e[name] = np.isin(p["pdg"], codes).sum()
            e["primary_ke_sum"] = p["ke"].sum()
            e["num_segments"] = row["num_steps"]
            start += len(p)
    particles = np.concatenate(chunks) if chunks else np.zeros(0, PARTICLE_DTYPE)
    return events, particles


def write_job_summary(path, job_info, events, particles):
    job = _empty(1, JOB_DTYPE)
    for k, v in job_info.items():
        if v is None:
            continue
        job[k][0] = v.encode()[:64] if isinstance(v, str) else v
    job["n_events"] = len(events)
    job["event_start"], job["event_end"] = 0, len(events)
    tmp = path + ".part.%d" % os.getpid()
    with h5py.File(tmp, "w") as f:
        f.attrs["version"] = VERSION
        for name, data in (("job", job), ("event", events), ("particle", particles)):
            f.create_dataset(name, data=data, compression="gzip", compression_opts=4)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- merging

def _empty(n, dtype):
    """Zero-filled table with float columns set to NaN (= not measured)."""
    a = np.zeros(n, dtype)
    for name in dtype.names:
        if dtype[name].kind == "f":
            a[name] = np.nan
    return a


def _conform(data, dtype):
    """Convert a table written by an older version to the current dtype."""
    if data.dtype == dtype:
        return data
    out = _empty(len(data), dtype)
    for name in dtype.names:
        if name in data.dtype.names:
            out[name] = data[name]
    return out


def _create_merged(path, campaign, stage):
    f = h5py.File(path, "w")
    f.attrs.update({"version": VERSION, "campaign": campaign, "stage": stage,
                    "n_job": 0, "n_event": 0, "n_particle": 0})
    for name, dt in TABLES:
        f.create_dataset(name, shape=(0,), maxshape=(None,), dtype=dt,
                         chunks=(max(1, (1 << 20) // dt.itemsize),),
                         compression="gzip", compression_opts=4)
    return f


def _append(f, name, data):
    ds = f[name]
    n = int(f.attrs["n_" + name])
    ds.resize((n + len(data),))
    ds[n:] = data
    return n


def merge(output, entries, campaign, stage, rebuild=False, log=print):
    """entries: list of (job_id, attempt, per-job summary path) for successful jobs.

    Appends jobs not yet in `output`. Rebuilds from scratch if requested or if
    the merged file holds a job/attempt that is no longer in `entries`.
    """
    wanted = {int(j): (int(a), p) for j, a, p in entries}
    if os.path.exists(output) and not rebuild:
        try:
            with h5py.File(output, "r") as f:
                if int(f.attrs.get("version", 0)) != VERSION:
                    raise KeyError("format version %s != %d" % (f.attrs.get("version"), VERSION))
                n = int(f.attrs["n_job"])
                have = {int(r["job_id"]): int(r["attempt"]) for r in f["job"][:n]}
        except (OSError, KeyError) as e:
            log("cannot read existing %s (%s); rebuilding" % (output, e))
            have, rebuild = {}, True
        stale = [j for j, a in have.items() if wanted.get(j, (None,))[0] != a]
        if stale:
            log("merged file has %d job(s) no longer current (e.g. job %d); rebuilding"
                % (len(stale), stale[0]))
            rebuild = True
    if rebuild or not os.path.exists(output):
        tmp = output + ".rebuild.%d" % os.getpid()
        f = _create_merged(tmp, campaign, stage)
        have = {}
    else:
        tmp = None
        f = h5py.File(output, "a")
        for name, _ in TABLES:   # drop rows of an interrupted merge
            f[name].resize((int(f.attrs["n_" + name]),))

    todo = sorted(j for j in wanted if j not in have)
    added = 0
    try:
        for j in todo:
            attempt, p = wanted[j]
            with h5py.File(p, "r") as s:
                job = _conform(s["job"][:], JOB_DTYPE)
                ev = _conform(s["event"][:], EVENT_DTYPE)
                part = _conform(s["particle"][:], PARTICLE_DTYPE)
            if len(job) != 1 or int(job[0]["job_id"]) != j:
                raise SummaryError("%s: unexpected job table" % p)
            ev_off = int(f.attrs["n_event"])
            pa_off = int(f.attrs["n_particle"])
            ev["particle_start"] += pa_off
            ev["particle_end"] += pa_off
            job["event_start"] += ev_off
            job["event_end"] += ev_off
            _append(f, "particle", part)
            _append(f, "event", ev)
            _append(f, "job", job)
            # commit: counts are updated only after all three tables are written
            f.attrs["n_particle"] = pa_off + len(part)
            f.attrs["n_event"] = ev_off + len(ev)
            f.attrs["n_job"] = int(f.attrs["n_job"]) + 1
            added += 1
            if added % 500 == 0:
                f.flush()
                log("  merged %d/%d" % (added, len(todo)))
        n_job, n_ev = int(f.attrs["n_job"]), int(f.attrs["n_event"])
    finally:
        f.close()
    if tmp:
        os.replace(tmp, output)
    return added, n_job, n_ev


# --------------------------------------------------------------------------- downstream stages

# One row per task of a downstream stage (e.g. JAXTPC), built from the workers'
# JSON summaries. A task processes the consecutive stage-1 jobs [first_job, last_job].
TASK_DTYPE = np.dtype([
    ("task_id", "i4"), ("attempt", "i2"), ("first_job", "i4"), ("last_job", "i4"),
    ("n_input_jobs", "i4"), ("n_input_events", "i4"), ("n_events", "i4"),
    ("n_files", "i4"), ("output_bytes", "i8"),
    ("start_time", "f8"), ("duration_s", "f4"), ("node", "S64"), ("slurm_job_id", "S32"),
    ("max_rss_mb", "f4"), ("avg_rss_mb", "f4"),
    ("n_gpus", "i2"), ("gpu_util_pct", "f4"), ("gpu_mem_used_mb", "f4"),
    ("gpu_mem_max_mb", "f4"), ("gpu_mem_total_mb", "f4"),
    ("seed", "i8"),
])


def task_row(summary):
    """One TASK_DTYPE row from a worker JSON summary (dict)."""
    r = _empty(1, TASK_DTYPE)
    res = summary.get("resources") or {}
    vals = {
        "task_id": summary["task_id"], "attempt": summary["attempt"],
        "first_job": summary["first_job"], "last_job": summary["last_job"],
        "n_input_jobs": summary.get("n_input_jobs"),
        "n_input_events": summary.get("expected_events"),
        "n_events": summary.get("n_events"),
        "n_files": len(summary.get("files") or []),
        "output_bytes": sum(f.get("size") or 0 for f in summary.get("files") or []),
        "start_time": summary.get("start"), "duration_s": summary.get("wall_s"),
        "node": summary.get("host") or "", "slurm_job_id": summary.get("slurm_job_id") or "",
        "max_rss_mb": summary.get("max_rss_mb"), "avg_rss_mb": res.get("avg_rss_mb"),
        "n_gpus": res.get("n_gpus", 0), "gpu_util_pct": res.get("gpu_util_pct"),
        "gpu_mem_used_mb": res.get("gpu_mem_used_mb"), "gpu_mem_max_mb": res.get("gpu_mem_max_mb"),
        "gpu_mem_total_mb": res.get("gpu_mem_total_mb"),
        "seed": (summary.get("seeds") or {}).get("task"),
    }
    for k, v in vals.items():
        if v is None:
            if TASK_DTYPE[k].kind in "iu":
                r[k][0] = -1          # integer "not available"
            continue
        r[k][0] = v.encode()[:TASK_DTYPE[k].itemsize] if isinstance(v, str) else v
    return r


def write_task_table(output, summary_paths, campaign, stage):
    """(Re)write <output> with a /job table of the given task summaries, sorted by task."""
    import json
    rows = []
    for p in summary_paths:
        with open(p) as f:
            rows.append(task_row(json.load(f)))
    table = np.concatenate(rows) if rows else np.zeros(0, TASK_DTYPE)
    table = table[np.argsort(table["task_id"], kind="stable")]
    tmp = output + ".part.%d" % os.getpid()
    with h5py.File(tmp, "w") as f:
        f.attrs.update({"version": VERSION, "campaign": campaign, "stage": stage,
                        "n_job": len(table)})
        f.create_dataset("job", data=table, compression="gzip", compression_opts=4)
    os.replace(tmp, output)
    return len(table)


def main(argv=None):
    ap = argparse.ArgumentParser(description="merge per-job summary HDF5 files")
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("tasks", help="job table of a downstream stage from JSON summaries")
    t.add_argument("--list", required=True, help="text file: one JSON summary path per line")
    t.add_argument("--output", required=True)
    t.add_argument("--campaign", required=True)
    t.add_argument("--stage", required=True)
    m = sub.add_parser("merge")
    m.add_argument("--list", required=True, help="text file: job_id attempt path per line")
    m.add_argument("--output", required=True)
    m.add_argument("--campaign", required=True)
    m.add_argument("--stage", required=True)
    m.add_argument("--rebuild", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "tasks":
        with open(a.list) as f:
            paths = [line.strip() for line in f if line.strip()]
        n = write_task_table(a.output, paths, a.campaign, a.stage)
        print("%s: wrote %d task(s) to %s" % (a.stage, n, a.output))
        return 0
    with open(a.list) as f:
        entries = [line.split() for line in f if line.strip()]
    added, n_job, n_ev = merge(a.output, entries, a.campaign, a.stage, a.rebuild)
    print("merged %d new job(s); %s now holds %d job(s), %d event(s)" % (
        added, a.output, n_job, n_ev))
    return 0


if __name__ == "__main__":
    sys.exit(main())
