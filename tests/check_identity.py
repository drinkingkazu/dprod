"""Check the event identity in every output of a campaign (tests/run_chain_test.sh).

  python3 tests/check_identity.py <campaign dir> <events_per_job> [<origin campaign>]

* every .h5 under data/ has /dprod naming this campaign, its stage and task;
  /dprod/events: event_id == job_id * events_per_job + job_event, unique per file
* edep-sim files (stage 1, or stage-1 files of the origin campaign re-labelled
  by stages.link_edepsim): /dprod/events is in the file's event order
  (= event/geant4 rows); link files link everything but /dprod, /provenance
* JAXTPC files (entry_NNNNNN): every entry carries job_id/job_event/event_id
  equal to those of its input event, and the entries are the /dprod events
* pysupera files: events/{job_id,job_event,event_id} are those of the edep-sim
  inputs, in input order
* the same event has the same event_id in every stage
"""
import glob
import os
import sys

import h5py
import numpy as np

cdir, epj = sys.argv[1], int(sys.argv[2])
tag = os.path.basename(cdir.rstrip("/"))
origin = sys.argv[3] if len(sys.argv) > 3 else tag
by_event = {}          # (job, job_event) -> {event_id seen}
n_files = 0
stages = set()


def ids_of(g):
    ev = g["events"]
    return [tuple(int(x) for x in row) for row in
            zip(ev["job_id"][()], ev["job_event"][()], ev["event_id"][()])]


for path in sorted(glob.glob(os.path.join(cdir, "data", "**", "*.h5"), recursive=True)):
    rel = os.path.relpath(path, cdir)
    with h5py.File(path, "r") as f:
        assert "dprod" in f, "%s: no /dprod" % rel
        d = f["dprod"]
        a = dict(d.attrs)
        assert a["campaign"] == tag and a["origin_campaign"] == origin, (rel, a)
        assert a["format"] == "dprod-identity/1", (rel, a)
        stage = a["stage"]
        stages.add(stage)
        rows = ids_of(d)
        assert len(rows) == a["n_events"] and len(set(r[2] for r in rows)) == len(rows), rel
        for j, e, eid in rows:
            assert eid == j * epj + e, (rel, j, e, eid)
            assert a["first_job"] <= j <= a["last_job"], (rel, j)
            by_event.setdefault((j, e), set()).add(eid)
        if "event/geant4" in f:
            t = f["event/geant4"][()]
            assert d["events"].attrs["order"] == "file"
            if "dprod_linked_from" in f.attrs:
                for k in f:
                    link = f.get(k, getlink=True)
                    assert isinstance(link, h5py.ExternalLink) == (k not in ("dprod", "provenance")), (rel, k)
            assert [(r[0], r[1]) for r in rows] == list(zip(t["run_id"].tolist(), t["event_id"].tolist())), rel
        elif any(k.startswith("entry_") for k in f):
            assert int(f["config"].attrs["format_version"]) == 2, rel
            got = sorted((int(f[k].attrs["job_id"]), int(f[k].attrs["job_event"]),
                          int(f[k].attrs["event_id"])) for k in f if k.startswith("entry_"))
            assert got == sorted(rows), (rel, got, rows)
            srcs = [s.decode() if isinstance(s, bytes) else s for s in f["config/source_files"][()]]
            for k in (k for k in f if k.startswith("entry_")):
                src = srcs[int(f[k].attrs["source_index"])]
                with h5py.File(src, "r") as fs:
                    i = int(f[k].attrs["source_event_idx"])
                    want = tuple(int(fs["dprod/events"][c][i]) for c in ("job_id", "job_event", "event_id"))
                have = tuple(int(f[k].attrs[c]) for c in ("job_id", "job_event", "event_id"))
                assert have == want, (rel, k, have, want)
        elif "events/job_event" in f:
            ev = f["events"]
            have = list(zip(ev["job_id"][()].tolist(), ev["job_event"][()].tolist(),
                            ev["event_id"][()].tolist()))
            assert sorted(have) == sorted(rows), rel
            assert have == sorted(have), (rel, "pysupera events not in input (job) order")
            ins = [s.decode() if isinstance(s, bytes) else s for s in f["inputs/edepsim"][()]]
            for (j, e, eid), k, i in zip(have, ev["input"][()], ev["input_event"][()]):
                with h5py.File(ins[k], "r") as fs:
                    want = tuple(int(fs["dprod/events"][c][i]) for c in ("job_id", "job_event", "event_id"))
                assert (j, e, eid) == want, (rel, (j, e, eid), want)
            assert (ev["jaxtpc_entry"][()] >= 0).all(), rel
        else:
            raise AssertionError("%s: neither edep-sim, JAXTPC format 2 nor pysupera 4.1" % rel)
    n_files += 1

assert all(len(v) == 1 for v in by_event.values()), "an event has different event_ids"
print("OK: %d files in %d stages (%s), %d events, one event_id each"
      % (n_files, len(stages), ", ".join(sorted(stages)), len(by_event)))
