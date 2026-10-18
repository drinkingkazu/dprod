"""Event and file identity: the /dprod group every output file carries.

Written by the worker into every HDF5 output, of every stage and role:

  /dprod                @format         "dprod-identity/1"
                        @campaign       the campaign that produced this file
                        @origin_campaign  the campaign whose stage 1 generated
                                        the events (differs for a derived campaign)
                        @stage @alias @role @task_id @attempt
                        @first_job @last_job  the task's stage-1 job range
                        @n_events       events in this file
                        @events_per_job of stage 1 (defines event_id)
                        @created @host @dprod_version
    events/job_id       (n_events,) int64  stage-1 job that generated the event
    events/job_event    (n_events,) int64  the event's number within that job
    events/event_id     (n_events,) int64  job_id * events_per_job + job_event:
                                    unique within the origin campaign, so
                                    (origin_campaign, event_id) names an event
                        @order  "file": row i is the file's event i (stage 1,
                                where downstream stages read the ids from);
                                "sorted": sorted by event_id (other stages:
                                which entry is which event is in the stage's
                                own format, e.g. JAXTPC entry attributes)

`python3 -m doraemon_prod.identity dump <file.h5> ...` prints the identity
(plus a provenance summary) as JSON; `dprod audit` shows it and checks it
against the bookkeeping.
"""

import json
import os
import socket
import sys
import time

GROUP = "dprod"
FORMAT = "dprod-identity/1"


def event_id(job_id, job_event, events_per_job):
    """The campaign-unique event id of event `job_event` of stage-1 job `job_id`."""
    return int(job_id) * int(events_per_job) + int(job_event)


def split_event_id(eid, events_per_job):
    """(job_id, job_event) of a campaign-unique event id."""
    return divmod(int(eid), int(events_per_job))


def write(path, m, task, role, pairs, order="sorted"):
    """Write /dprod into `path`. pairs: [(job_id, job_event)] -- in the file's
    event order for order="file" (stage 1), any order for "sorted". Replaces an
    existing /dprod (a retried attempt). No-op for manifests from before
    identities (no "identity" entry)."""
    import h5py
    import numpy as np
    info = m.get("identity")
    if not info:
        return False
    epj = int(info["events_per_job"])
    pairs = [(int(j), int(e)) for j, e in pairs]
    if order == "sorted":
        pairs.sort()
    job = np.array([p[0] for p in pairs], dtype=np.int64)
    jev = np.array([p[1] for p in pairs], dtype=np.int64)
    with h5py.File(path, "a") as f:
        if GROUP in f:
            del f[GROUP]
        g = f.create_group(GROUP)
        attrs = {"format": FORMAT, "campaign": m["campaign"],
                 "origin_campaign": info["origin_campaign"], "stage": m["stage"],
                 "alias": m["stage_config"].get("alias") or "", "role": role,
                 "task_id": int(task["task_id"]), "attempt": int(task["attempt"]),
                 "first_job": int(task["first_job"]), "last_job": int(task["last_job"]),
                 "n_events": len(pairs), "events_per_job": epj,
                 "created": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                 "host": socket.gethostname(), "dprod_version": _version()}
        for k, v in attrs.items():
            g.attrs[k] = v
        ev = g.create_group("events")
        ev.attrs["order"] = order
        ev.create_dataset("job_id", data=job)
        ev.create_dataset("job_event", data=jev)
        ev.create_dataset("event_id", data=job * epj + jev)
    return True


def _version():
    from . import __version__
    return __version__


def read(path):
    """The identity of a file as a dict (attrs + 'events' arrays as lists), or None."""
    import h5py
    with h5py.File(path, "r") as f:
        if GROUP not in f:
            return None
        g = f[GROUP]
        d = {k: _py(v) for k, v in g.attrs.items()}
        ev = g.get("events")
        if ev is not None:
            d["order"] = _py(ev.attrs.get("order", ""))
            d["events"] = {k: [int(x) for x in ev[k][()]] for k in ("job_id", "job_event", "event_id")
                           if k in ev}
        return d


def _py(v):
    if isinstance(v, bytes):
        return v.decode()
    if hasattr(v, "item"):
        return v.item()
    return v


def provenance_summary(path):
    """The stage chain and per-stage software commits of a file's /provenance."""
    from .provenance import read_provenance, task_blocks
    try:
        prov = read_provenance(path)
    except Exception as e:     # an unreadable provenance must not hide the identity
        return {"error": "%s: %s" % (type(e).__name__, e)}
    if prov is None:
        return None
    stages = prov.get("stages") or []
    if isinstance(stages, str):
        stages = [stages]
    out = {"campaign": prov.get("campaign"), "stages": list(stages), "software": {}}
    for st in stages:
        sw = {}
        for blk in task_blocks(prov, st):
            for name, info in (blk.get("software") or {}).items():
                if isinstance(info, dict):
                    v = info.get("git_describe")
                    if not v:
                        v = str(info.get("git_commit") or info.get("sha256") or "")[:12]
                        if v and info.get("git_dirty") in (True, 1, "True"):
                            v += "-dirty"
                    if v:
                        sw.setdefault(name, set()).add(str(v))
        out["software"][st] = {k: sorted(v) for k, v in sw.items()}
    return out


def dump(paths):
    """[{path, identity, provenance, error}] for the given files (needs h5py)."""
    res = []
    for p in paths:
        r = {"path": os.path.abspath(p)}
        try:
            r["identity"] = read(p)
            r["provenance"] = provenance_summary(p)
        except Exception as e:
            r["error"] = "%s: %s" % (type(e).__name__, e)
        res.append(r)
    return res


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) < 2 or argv[0] != "dump":
        print("usage: python3 -m doraemon_prod.identity dump <file.h5> ...", file=sys.stderr)
        return 2
    json.dump(dump(argv[1:]), sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
