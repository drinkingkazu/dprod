"""Test `dprod destroy`.

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_destroy.py <scratch dir>
"""
import json
import os
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import campaign as K, cli, db as D, config as C  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(os.path.join(ROOT, "fail"))
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ["DPROD_STATE"] = ""           # do not remember campaigns in ~/.config
os.environ["DPROD_BATCH"] = "1"          # no confirmation prompts in tests


class Deferred(LocalScheduler):
    """Submitted elements stay PENDING; cancel marks them CANCELLED."""
    cancelled = []

    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        json.dump({str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None, "start": None,
                            "end": None, "node": None} for i in range(n_tasks)},
                  open(self._state_path(jid), "w"))
        return jid

    def cancel(self, jid, indices=None):
        p = self._state_path(jid)
        st = json.load(open(p))
        for k in st:
            if indices is None or int(k) in indices:
                st[k]["state"] = "CANCELLED"
                Deferred.cancelled.append((jid, int(k)))
        json.dump(st, open(p, "w"))


K.get_scheduler = lambda site, cdir: Deferred(site, os.path.join(cdir, "local_sched"))

text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
text = text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1)
site_a = os.path.join(ROOT, "site_a.yaml")                       # web dir per campaign, outside
open(site_a, "w").write(text + "\nweb:\n  dir: %s/www/{campaign}\n" % ROOT)
site_b = os.path.join(ROOT, "site_b.yaml")                       # shared web dir (no tag)
open(site_b, "w").write(text + "\nweb:\n  dir: %s/www_shared\n" % ROOT)
base = open(os.path.join(HERE, "campaign_local_test.yaml")).read().replace("n_jobs: 6", "n_jobs: 3")


def mk(tag, site):
    cfg = os.path.join(ROOT, tag + ".yaml")
    open(cfg, "w").write(base.replace("campaign: localtest_doraemon_v0.0", "campaign: " + tag))
    assert cli.main(["--site", site, "init", cfg]) == 0
    assert cli.main(["--site", site, "--campaign", tag, "submit", "1"]) == 0     # queued (deferred)
    assert cli.main(["--site", site, "--campaign", tag, "web", "--no-sync"]) == 0


def d(site, tag, *args):
    print("+ dprod %s" % " ".join(args))
    return cli.main(["--site", site, "--campaign", tag] + list(args))


mk("desttest", site_a)
mk("keepme", site_a)
mk("prod_doraemon_x", site_a)
mk("sharedweb", site_b)
cdir = os.path.join(ROOT, "storage", "desttest")
logdir = os.path.join(ROOT, "joblog", "desttest")
webdir = os.path.join(ROOT, "www", "desttest")
assert all(os.path.isdir(p) for p in (cdir, logdir, webdir))

assert d(site_a, "desttest", "destroy", "--dry-run") == 0
assert os.path.isdir(cdir)
assert d(site_a, "desttest", "destroy") == 1                     # stdin is not a terminal
assert d(site_a, "desttest", "destroy", "--confirm", "desttes") == 1
assert os.path.isdir(cdir) and os.path.isdir(webdir)
print("OK: dry-run, missing and wrong confirmation delete nothing")

assert d(site_a, "prod_doraemon_x", "destroy", "--confirm", "prod_doraemon_x") == 1
assert d(site_a, "prod_doraemon_x", "destroy", "--confirm", "prod_doraemon_x", "--allow-production") == 0
assert not os.path.exists(os.path.join(ROOT, "storage", "prod_doraemon_x"))
print("OK: prod_ campaigns need --allow-production")

Deferred.cancelled[:] = []
assert d(site_a, "desttest", "destroy", "--confirm", "desttest") == 0
assert len(Deferred.cancelled) == 3, Deferred.cancelled                  # its 3 queued jobs
assert not any(os.path.exists(p) for p in (cdir, logdir, webdir)), [p for p in (cdir, logdir, webdir) if os.path.exists(p)]
assert os.path.isdir(os.path.join(ROOT, "storage", "keepme")) and os.path.isdir(os.path.join(ROOT, "www", "keepme"))
assert os.path.isdir(os.path.join(ROOT, "storage")) and os.path.isdir(os.path.join(ROOT, "joblog"))
reg = json.load(open(os.path.join(ROOT, "www", "campaigns.json")))["campaigns"]
assert "desttest" not in reg and "keepme" in reg, sorted(reg)
assert "keepme" in open(os.path.join(ROOT, "www", "index.html")).read()
print("OK: destroy cancelled the queued jobs and removed campaign dir, log dir and web dir; others untouched")
print("OK: the campaign is gone from campaigns.json and the all-campaigns page")

assert d(site_b, "sharedweb", "destroy", "--confirm", "sharedweb") == 0
assert not os.path.exists(os.path.join(ROOT, "storage", "sharedweb"))
assert os.path.isdir(os.path.join(ROOT, "www_shared"))                   # not campaign-specific
print("OK: a shared web directory (no campaign tag in its path) is kept")
assert d(site_a, "desttest", "status") == 1                              # gone
print("DESTROY TEST OK")
