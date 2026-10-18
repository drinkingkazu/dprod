"""Edep-sim files of another campaign, as this campaign's own, without copying.

    python3 -m doraemon_prod.stages.link_edepsim --outdir <dir> <edep-sim files>

For a derived campaign whose first own stage re-labels the inherited stage-1
output (e.g. to give files written before dprod's /dprod existed their
identity): per input file, a small file of the same name in <dir> whose
top-level groups are HDF5 external links to the input's. The worker then
writes this campaign's /dprod and /provenance into it, so downstream stages
read the events through the links and the identity from the link file.

Not linked: /dprod and /provenance (the link file has its own; writing them
through a link would write into the input). h5py (and so JAXTPC and pysupera)
follows the links when reading; the input files must stay where they are.

Each input must hold one job's events 0..n-1 in order (as stage 1 writes
them), so the link file's /dprod, written in file order, is that of the input.
"""

import argparse
import os
import sys

SKIP = ("dprod", "provenance")
TABLE = "event/geant4"


def link(src, dst):
    """Write dst linking to src's top-level groups; returns the number of events."""
    import h5py
    import numpy as np
    src = os.path.abspath(src)
    with h5py.File(src, "r") as f:
        if TABLE not in f:
            raise ValueError("%s: no %s" % (src, TABLE))
        t = f[TABLE][()]
        names = [k for k in f if k not in SKIP]
        attrs = dict(f.attrs)
    jobs = set(t["run_id"].tolist())
    if len(t) == 0 or len(jobs) != 1 or not np.array_equal(t["event_id"], np.arange(len(t))):
        raise ValueError("%s: events are not one job's 0..n-1 in order (run_id %s, event_id %s...)"
                         % (src, sorted(jobs)[:5], t["event_id"][:5].tolist()))
    with h5py.File(dst, "w") as g:
        for k, v in attrs.items():
            g.attrs[k] = v
        g.attrs["dprod_linked_from"] = src
        for k in names:
            g[k] = h5py.ExternalLink(src, "/" + k)
    with h5py.File(dst, "r") as g:          # the links resolve, and to the same events
        if not np.array_equal(g[TABLE][()], t):
            raise ValueError("%s: %s read through the links differs from %s" % (dst, TABLE, src))
    return len(t)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--outdir", required=True)
    ap.add_argument("inputs", nargs="+")
    a = ap.parse_args(argv)
    names = [os.path.basename(p) for p in a.inputs]
    if len(set(names)) != len(names):
        sys.exit("input file names are not unique: %s" % names)
    os.makedirs(a.outdir, exist_ok=True)
    for src, name in zip(a.inputs, names):
        try:
            n = link(src, os.path.join(a.outdir, name))
        except (OSError, ValueError, KeyError) as e:
            sys.exit("link_edepsim: %s" % e)
        print("%s -> %s (%d events)" % (src, name, n), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
