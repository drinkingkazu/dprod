"""Test the submission summary + yes/no confirmation (and --batch).

  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_confirm.py <scratch dir>
"""
import builtins
import io
import json
import os
import shutil
import sys
import time
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import campaign as K, cli, db as D, config as C  # noqa: E402
from doraemon_prod.scheduler import LocalScheduler  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(os.path.join(ROOT, "fail"))
os.environ["DPROD_LOCAL_ROOT"] = ROOT
os.environ.pop("DPROD_BATCH", None)


class Pending(LocalScheduler):          # submissions stay queued; nothing runs
    def submit(self, script, n_tasks, max_concurrent=None):
        jid = str(int(time.time() * 1e6) % 10**10)
        json.dump({str(i): {"state": "PENDING", "exit_code": "0:0", "elapsed_s": None, "start": None,
                            "end": None, "node": None} for i in range(n_tasks)}, open(self._state_path(jid), "w"))
        return jid


K.get_scheduler = lambda site, cdir: Pending(site, os.path.join(cdir, "local_sched"))
site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(HERE, "..", "configs", "sites", "local.yaml")).read()
open(site, "w").write(text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1))
cfg = os.path.join(ROOT, "c.yaml")
open(cfg, "w").write(open(os.path.join(HERE, "campaign_local_test.yaml")).read().replace(
    "campaign: localtest_doraemon_v0.0", "campaign: conftest"))
TAG = "conftest"


def run(args, answer=None, tty=True):
    sys.stdin.isatty = lambda: tty
    asked = []
    builtins.input = lambda prompt="": asked.append(prompt) or answer
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = cli.main(["--site", site, "--campaign", TAG] + args)
    return rc, buf.getvalue(), asked


def n_submitted():
    c = K.Campaign(C.load_site(site), TAG)
    return len(D.tasks(c.con, "edepsim", ["submitted"]))


assert cli.main(["--site", site, "init", cfg]) == 0

rc, out, asked = run(["submit", "1", "--limit", "5", "--partition", "roma", "--account", "neutrino:x"], answer="n")
print(out)
assert rc == 0 and asked == ["Proceed? [y/N] "] and n_submitted() == 0 and "not submitted" in out
assert "roma" in out and "neutrino:x" in out and "5 (0-4)" in out and "15" in out   # 5 jobs x 3 events
assert "total: 5 task(s) in 2 array(s), 15 events" in out                           # max_array_size 4
print("OK: summary shown (tasks, arrays, jobs, events, partition/account); 'n' submits nothing")

rc, out, asked = run(["submit", "1", "--limit", "5"], answer="y")
assert rc == 0 and asked and n_submitted() == 5
print("OK: 'y' submits exactly the planned 5 tasks")

rc, out, asked = run(["submit", "1"], tty=False)
assert rc == 1 and not asked and n_submitted() == 5
rc, out, asked = run(["submit", "1", "-y"], tty=False)
assert rc == 0 and not asked and n_submitted() == 6 and "Submission plan" in out
print("OK: no terminal -> refused without --batch; -y submits without asking (summary still printed)")

rc, out, asked = run(["submit", "1", "--dry-run"], answer="y")
assert rc == 0 and not asked
os.environ["DPROD_BATCH"] = "1"
rc, out, asked = run(["advance"], tty=False)
assert rc == 0 and not asked and "nothing ready to submit" in out
print("OK: --dry-run and DPROD_BATCH=1 do not ask")
print("CONFIRM TEST OK")
