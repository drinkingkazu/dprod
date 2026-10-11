"""Test control_root: bookkeeping + lock outside the campaign (storage) dir, moving an
existing campaign's bookkeeping there, and status/cancel when the bookkeeping cannot
be written (as on a full disk; simulated with a read-only database file).

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_control_root.py <scratch dir>

Jobs never run: submitted elements stay PENDING until cancelled.
"""
import contextlib
import io
import json
import os
import shutil
import stat
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import campaign as K  # noqa: E402
from doraemon_prod import cli, config as C, db as D  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(ROOT)
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""
os.environ["DPROD_BATCH"] = "1"


class PendingScheduler(LocalScheduler):
    """Elements stay PENDING; cancel() makes them CANCELLED (like scancel)."""
    cancelled = []

    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        json.dump({str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None,
                            "start": None, "end": None, "node": None} for i in range(n_tasks)},
                  open(self._state_path(jid), "w"))
        return jid

    def cancel(self, array_job_id, indices=None):
        p = self._state_path(array_job_id)
        states = json.load(open(p))
        for i in (indices if indices is not None else [int(k) for k in states]):
            states[str(i)]["state"] = "CANCELLED"
            PendingScheduler.cancelled.append((array_job_id, i))
        json.dump(states, open(p, "w"))


K.get_scheduler = lambda site, cdir: PendingScheduler(site, os.path.join(cdir, "local_sched"))

base = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
base = base.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, ROOT), 1)
site_old = os.path.join(ROOT, "site_old.yaml")          # no control_root
site_new = os.path.join(ROOT, "site_new.yaml")          # with control_root
CTL = os.path.join(ROOT, "control")
open(site_old, "w").write(base)
open(site_new, "w").write(base + "\ncontrol_root: %s\n" % CTL)


def config(tag):
    path = os.path.join(ROOT, tag + ".yaml")
    text = open(os.path.join(HERE, "campaign_local_test.yaml")).read()
    open(path, "w").write(text.replace("campaign: localtest_doraemon_v0.0", "campaign: " + tag))
    return path


def dprod(site, tag, *args, rc=0):
    print("+ dprod " + " ".join(args))
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        got = cli.main(["--site", site, "--campaign", tag] + list(args))
    print(out.getvalue() + err.getvalue(), end="")
    assert got == rc, (got, rc)
    return out.getvalue(), err.getvalue()


def statuses(site, tag, stage="edepsim"):
    c = K.Campaign(C.load_site(site), tag)
    return [t["status"] for t in D.tasks(c.con, stage)]


# 1. old layout; the bookkeeping becomes unwritable: status and cancel still work
TAG = "ctltest_old"
dprod(site_old, TAG, "init", config(TAG))
cdir = os.path.join(ROOT, "storage", TAG)
db_old = os.path.join(cdir, "bookkeeping.sqlite")
assert os.path.exists(db_old) and not os.path.exists(CTL)
dprod(site_old, TAG, "submit", "1", "--limit", "3")
assert statuses(site_old, TAG)[:3] == ["submitted"] * 3

os.chmod(db_old, stat.S_IRUSR)                 # writes now fail like on a full disk
out, err = dprod(site_old, TAG, "status")
assert "could not update the bookkeeping" in err and "edepsim" in out, err
out, err = dprod(site_old, TAG, "cancel", "1")
assert len(PendingScheduler.cancelled) == 3, PendingScheduler.cancelled
assert "bookkeeping was not updated" in out, out
print("OK: status and cancel work while the bookkeeping cannot be written")

os.chmod(db_old, stat.S_IRUSR | stat.S_IWUSR)  # space again: sync records the cancellation
dprod(site_old, TAG, "status")
assert statuses(site_old, TAG)[:3] == ["new"] * 3, statuses(site_old, TAG)
c = K.Campaign(C.load_site(site_old), TAG)
assert c.attempts_used("edepsim", 0) == 0      # a cancellation does not use an attempt
print("OK: the next sync records the cancelled elements")

# 2. the site gets control_root: the existing campaign keeps working, then moves there
c = K.Campaign(C.load_site(site_new), TAG)
assert c.db_path == db_old and c.ctl_dir == cdir, (c.db_path, c.ctl_dir)
dprod(site_new, TAG, "submit", "1", "--limit", "2")
dprod(site_old, TAG, "relocate-bookkeeping", rc=1)      # site without control_root: refused
dprod(site_new, TAG, "relocate-bookkeeping")
db_new = os.path.join(CTL, TAG, "bookkeeping.sqlite")
assert os.path.exists(db_new) and not os.path.exists(db_old) and os.path.exists(db_old + ".moved")
assert statuses(site_new, TAG)[:2] == ["submitted"] * 2
dprod(site_new, TAG, "relocate-bookkeeping", rc=1)      # already moved
out, err = dprod(site_old, TAG, "status", rc=1)          # stale site config: clear error
assert "relocate-bookkeeping" in err, err
dprod(site_new, TAG, "cancel", "1")
assert os.path.exists(os.path.join(CTL, TAG, ".dprod.lock"))
print("OK: relocate-bookkeeping moved db + lock; old location refuses with a hint")

# a long-running process that opened the old location notices the move
TAG2 = "ctltest_watch"
dprod(site_old, TAG2, "init", config(TAG2))
stale = K.Campaign(C.load_site(site_new), TAG2)          # still the old location
dprod(site_new, TAG2, "relocate-bookkeeping")
try:
    with stale.lock():
        raise AssertionError("lock taken on a moved bookkeeping")
except K.CampaignError as e:
    assert "start this command again" in str(e), e
print("OK: a process holding the old location stops instead of writing to the moved file")

# 3. a new campaign with control_root: nothing controller-only in the storage dir
TAG3 = "ctltest_new"
dprod(site_new, TAG3, "init", config(TAG3))
cdir3 = os.path.join(ROOT, "storage", TAG3)
assert os.path.exists(os.path.join(CTL, TAG3, "bookkeeping.sqlite"))
assert not os.path.exists(os.path.join(cdir3, "bookkeeping.sqlite"))
dprod(site_new, TAG3, "submit", "1", "--limit", "1")
assert not os.path.exists(os.path.join(cdir3, ".dprod.lock"))
out, _ = dprod(site_new, TAG3, "destroy", "--dry-run")
assert os.path.join(CTL, TAG3) in out, out
out, _ = dprod(site_new, TAG3, "campaigns")
assert TAG3 in out
dprod(site_new, TAG3, "destroy", "--confirm", TAG3)
assert not os.path.exists(os.path.join(CTL, TAG3)) and not os.path.exists(cdir3)
print("OK: new campaign keeps bookkeeping + lock under control_root; destroy removes it")
# 4. as on a full disk at S3DF: the campaign dir refuses writes, even opening the existing
#    lock file for writing (and renaming the old database)
TAG4 = "ctltest_full"
dprod(site_old, TAG4, "init", config(TAG4))
dprod(site_old, TAG4, "submit", "1", "--limit", "2")
cdir4 = os.path.join(ROOT, "storage", TAG4)
lock4 = os.path.join(cdir4, ".dprod.lock")
os.chmod(lock4, stat.S_IRUSR)
os.chmod(cdir4, stat.S_IRUSR | stat.S_IXUSR)
try:
    out, err = dprod(site_old, TAG4, "status")
    assert "using a shared lock" in out and "could not update the bookkeeping" in err, (out, err)
    n = len(PendingScheduler.cancelled)
    dprod(site_old, TAG4, "cancel", "1")
    assert len(PendingScheduler.cancelled) == n + 2
    print("OK: status and cancel work when the lock file cannot be opened for writing")
    _, err = dprod(site_new, TAG4, "relocate-bookkeeping", rc=1)
    assert "--keep-old" in err and not os.path.exists(os.path.join(CTL, TAG4, "bookkeeping.sqlite"))
    stale = K.Campaign(C.load_site(site_new), TAG4)      # e.g. a running watch
    out, _ = dprod(site_new, TAG4, "relocate-bookkeeping", "--keep-old")
    assert "left in place" in out and os.path.exists(os.path.join(cdir4, "bookkeeping.sqlite"))
    assert os.path.exists(os.path.join(CTL, TAG4, "bookkeeping.sqlite"))
    try:
        with stale.lock():
            raise AssertionError("lock taken on the old location after the move")
    except K.CampaignError as e:
        assert "start this command again" in str(e), e
    dprod(site_new, TAG4, "status")                 # new location: writes work again
    assert statuses(site_new, TAG4)[:2] == ["new"] * 2, statuses(site_new, TAG4)
    print("OK: relocate-bookkeeping --keep-old moves off a full campaign dir")
finally:
    os.chmod(cdir4, stat.S_IRWXU)
    os.chmod(lock4, stat.S_IRUSR | stat.S_IWUSR)
print("CONTROL_ROOT TEST OK")
