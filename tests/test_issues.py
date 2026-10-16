"""Test reading a stage's issues/ group (pysupera 4.x outputs) into the task summary.

  apptainer exec -B /home/kazu,/tmp /home/kazu/sw/images/test.sif python3 tests/test_issues.py <scratch dir>

An output with an issues/ group says what the stage noticed but did not stop for.
The worker turns it into a task warning (which becomes the task note), a per-file
"issues" entry and a per-task count in the summary, and the n_issues column of
the merged task table: -1 when no output had a group, 0 when they were all empty.
"""
import os
import shutil
import sys

import h5py
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod.idreaders import describe_issues, read_issues  # noqa: E402
from doraemon_prod.summary import task_row  # noqa: E402

CODES = [(1, "PRIMARY_TABLE_MISMATCH"), (2, "NO_PRIMARY_TABLE"), (3, "EMPTY_IMAGE"),
         (4, "OUTSIDE_READOUT_WINDOW")]


def write(path, rows=None):
    """A pysupera-like output: issues/ as pysupera writes it, or none at all."""
    s = h5py.string_dtype()
    with h5py.File(path, "w") as f:
        f["n_events"] = 1
        if rows is None:
            return
        g = f.create_group("issues")
        g["event"] = np.array([r[0] for r in rows], dtype=np.int64)
        g["code"] = np.array([r[1] for r in rows], dtype=np.int32)
        g["geant4_trackid"] = np.array([r[2] for r in rows], dtype=np.int64)
        g.create_dataset("message", data=np.array([r[3] for r in rows], dtype=object), dtype=s)
        g["codes/code"] = np.array([c for c, _ in CODES], dtype=np.int32)
        g.create_dataset("codes/name", data=np.array([n for _, n in CODES], dtype=object),
                         dtype=s)


def main(scratch):
    shutil.rmtree(scratch, ignore_errors=True)
    os.makedirs(scratch)
    p = lambda n: os.path.join(scratch, n)

    write(p("none.h5"))
    assert read_issues(p("none.h5")) is None
    assert describe_issues("none.h5", None) is None

    write(p("clean.h5"), [])
    clean = read_issues(p("clean.h5"))
    assert clean == {"total": 0, "by_code": {}, "first": []}, clean
    assert describe_issues("clean.h5", clean) is None

    rows = [(50, 1, 2, "track 2 with parent_track_id -1 but not in the primary table"),
            (51, 1, 7, "track 7 in the primary table but with a parent"),
            (-1, 2, -1, "the input has no primary table"),
            (9, 3, -1, "all 4 particle(s) have an empty point cloud")]
    write(p("bad.h5"), rows)
    bad = read_issues(p("bad.h5"))
    assert bad["total"] == 4
    assert bad["by_code"] == {"PRIMARY_TABLE_MISMATCH": 2, "NO_PRIMARY_TABLE": 1,
                              "EMPTY_IMAGE": 1}, bad
    assert bad["first"] == ["event 50: " + rows[0][3], "event 51: " + rows[1][3],
                            "file: " + rows[2][3]], bad["first"]
    line = describe_issues("data/x/bad.h5", bad)
    assert line.startswith("data/x/bad.h5: 4 issue(s): EMPTY_IMAGE=1, NO_PRIMARY_TABLE=1, "
                           "PRIMARY_TABLE_MISMATCH=2; first: event 50: "), line

    base = {"task_id": 0, "attempt": 1, "first_job": 0, "last_job": 1}
    files = lambda *iss: {**base, "files": [{"path": "f%d" % k, "size": 1, "issues": i}
                                            for k, i in enumerate(iss)]}
    assert task_row(files(None, None))["n_issues"][0] == -1      # no output had a group
    assert task_row(files(clean, None))["n_issues"][0] == 0
    assert task_row(files(clean, bad))["n_issues"][0] == 4
    assert task_row({**base, "files": [{"path": "f", "size": 1}]})["n_issues"][0] == -1
    print("ISSUES OK")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dprod_test_issues")
