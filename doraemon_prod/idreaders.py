"""Extract (job_id, event_id) pairs from output files (worker side, needs h5py).

Each reader takes (path, rel_path, options) and returns {job_id: [event_id, ...]}.
It must raise IdReadError if the ids cannot be determined unambiguously: a file
whose events cannot be traced back to (job id, event id) is a failed output.

Register new formats (e.g. pysupera output) in READERS.
"""

import os
import re

from .layout import parse_job_range


class IdReadError(Exception):
    pass


def _h5():
    import h5py  # imported lazily so the controller never needs h5py
    return h5py


def _collect(pairs):
    out = {}
    for j, e in pairs:
        out.setdefault(int(j), []).append(int(e))
    for j in out:
        ev = out[j]
        if len(set(ev)) != len(ev):
            raise IdReadError("duplicate event ids for job %d" % j)
        ev.sort()
    return out


def read_event_table(path, rel_path, options):
    """A compound dataset with run(job)/event id fields, e.g. edep-sim event/geant4.

    options: dataset (default event/geant4), job_field (run_id), event_field (event_id)
    """
    ds = options.get("dataset", "event/geant4")
    jf = options.get("job_field", "run_id")
    ef = options.get("event_field", "event_id")
    with _h5().File(path, "r") as f:
        if ds not in f:
            raise IdReadError("%s: no dataset %s" % (rel_path, ds))
        table = f[ds][:]
    if len(table) == 0:
        return {}
    return _collect(zip(table[jf], table[ef]))


_RUN_DIR_RE = re.compile(r"(?:^|/)run_(\d+)(?:/|$)")


def read_jaxtpc(path, rel_path, options):
    """JAXTPC sensor/step/hits files: one group per event (event_NNN, attr event_id).

    The job id is taken from, in order of preference and cross-checked:
      1. the `source_file` attribute of the `config` group (our edep-sim file
         name encodes the job range, e.g. edepsim_j000123-000123.h5)
      2. the run_<id> directory JAXTPC writes the file into
    Note: JAXTPC run_batch.py (--edepsim-ids) treats a run_id of 0 as unset and
    writes job 0 into run_unknown/, so (1) is required for job 0.
    """
    event_prefix = options.get("event_prefix", "event_")
    job_from_source = job_from_dir = None
    event_ids = []
    with _h5().File(path, "r") as f:
        cfg = f.get("config")
        src = cfg.attrs.get("source_file") if cfg is not None else None
        if src is not None:
            if isinstance(src, bytes):
                src = src.decode()
            rng = parse_job_range(os.path.basename(str(src)))
            if rng:
                if rng[0] != rng[1]:
                    raise IdReadError("%s: source %s spans several jobs" % (rel_path, src))
                job_from_source = rng[0]
        for key, grp in f.items():
            if not key.startswith(event_prefix):
                continue
            if "event_id" not in grp.attrs:
                raise IdReadError("%s: %s has no event_id attribute" % (rel_path, key))
            event_ids.append(int(grp.attrs["event_id"]))
    m = _RUN_DIR_RE.search(rel_path)
    if m:
        job_from_dir = int(m.group(1))
    if job_from_source is not None and job_from_dir is not None and job_from_source != job_from_dir:
        raise IdReadError("%s: job id mismatch (source_file says %d, run dir says %d)" % (
            rel_path, job_from_source, job_from_dir))
    job = job_from_source if job_from_source is not None else job_from_dir
    if job is None:
        raise IdReadError("%s: cannot determine job id" % rel_path)
    return _collect((job, e) for e in event_ids)


def read_pysupera(path, rel_path, options):
    """pysupera output tagged by doraemon_prod.stages.supera: events/job_id, events/event_id."""
    with _h5().File(path, "r") as f:
        if "events/job_id" not in f or "events/event_id" not in f:
            raise IdReadError("%s: no events/job_id + events/event_id (not tagged)" % rel_path)
        n = int(f["n_events"][()]) if "n_events" in f else None
        jobs, evs = f["events/job_id"][:], f["events/event_id"][:]
    if n is not None and not len(jobs) == len(evs) == n:
        raise IdReadError("%s: id arrays do not match n_events=%d" % (rel_path, n))
    return _collect(zip(jobs, evs))


READERS = {
    "edepsim": read_event_table,
    "event_table": read_event_table,
    "jaxtpc": read_jaxtpc,
    "pysupera": read_pysupera,
}


def read_ids(reader, path, rel_path, options=None):
    if reader not in READERS:
        raise IdReadError("unknown id_reader %r (known: %s)" % (reader, ", ".join(READERS)))
    try:
        return READERS[reader](path, rel_path, options or {})
    except IdReadError:
        raise
    except Exception as e:  # unreadable/corrupt file
        raise IdReadError("%s: %s: %s" % (rel_path, type(e).__name__, e))
