"""Test choosing site / config / campaign interactively and remembering the current campaign.

  apptainer exec -B /home/kazu,/tmp <test.sif> python3 tests/test_select.py <scratch dir>
"""
import builtins
import io
import json
import os
import shutil
import sys
from contextlib import redirect_stderr, redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from doraemon_prod import cli  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(ROOT)
os.environ["DPROD_LOCAL_ROOT"] = ROOT          # makes the repo's `local` site usable here
os.environ["DPROD_STATE"] = STATE = os.path.join(ROOT, "state.json")
for k in ("DPROD_SITE", "DPROD_CAMPAIGN", "DPROD_BATCH"):
    os.environ.pop(k, None)


def run(args, answers=(), tty=True):
    sys.stdin.isatty = lambda: tty
    queue = list(answers)
    prompts = []

    def fake_input(prompt=""):
        prompts.append(prompt)
        return queue.pop(0)
    builtins.input = fake_input
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        rc = cli.main(list(args))
    assert not queue, "unused answers %s" % queue
    return rc, out.getvalue(), err.getvalue(), prompts


def state():
    return json.load(open(STATE)) if os.path.exists(STATE) else {}


# listing
rc, out, _, _ = run(["sites"])
assert rc == 0 and "local" in out and "s3df" in out and "nersc" in out
assert [l for l in out.splitlines() if "local" in l and "usable on this machine" in l]
rc, out, _, _ = run(["configs"])
assert rc == 0 and "campaign: test_doraemon_2026_smoke_v0.0" in out
print("OK: `dprod sites` / `dprod configs` list the options (local usable here)")

# dprod init, fully interactive: Enter = default site (the one usable here), pick config, confirm
from doraemon_prod import select as SEL                      # noqa: E402
cfgs = [p for p, _, _ in SEL.list_configs()]
smoke = [i for i, p in enumerate(cfgs, 1) if "smoke" in p][0]
rc, out, err, prompts = run(["init"], answers=["", str(smoke), "y"])
print(out)
assert rc == 0, err
assert "campaign tag (from the config): test_doraemon_2026_smoke_v0.0" in out
assert os.path.exists(os.path.join(ROOT, "storage", "test_doraemon_2026_smoke_v0.0", "campaign.yaml"))
assert state()["campaign"] == "test_doraemon_2026_smoke_v0.0" and state()["site"] == "local"
print("OK: interactive init (site default, config menu, tag from the config, confirmation); now current")

# remembered campaign is used without any flags; a note says so
rc, out, err, prompts = run(["status", "--no-sync"])
assert rc == 0 and not prompts and "test_doraemon_2026_smoke_v0.0" in out and "remembered" in err
print("OK: `dprod status` uses the current campaign (note: %s)" % err.strip().splitlines()[-1])

# re-init of an existing tag is refused before asking anything more
rc, out, err, prompts = run(["init"], answers=[str(smoke)])
assert rc == 1 and "already exists" in err

# a second campaign via explicit config -> becomes current
v01 = [p for p in cfgs if p.endswith("test_doraemon_2026_v0.1.yaml")][0]
rc, out, err, _ = run(["init", v01, "--batch"])
assert rc == 0 and state()["campaign"] == "test_doraemon_2026_v0.1"
rc, out, _, _ = run(["campaigns"])
assert rc == 0 and "* test_doraemon_2026_v0.1" in out and "  test_doraemon_2026_smoke_v0.0" in out
print("OK: `dprod campaigns` lists both, current marked")

# explicit --campaign does not change the current one
rc, out, err, _ = run(["--campaign", "test_doraemon_2026_smoke_v0.0", "status", "--no-sync"])
assert rc == 0 and state()["campaign"] == "test_doraemon_2026_v0.1"
print("OK: an explicit --campaign leaves the current campaign alone (cron-safe)")

# dprod use: menu, and by name
rc, out, err, prompts = run(["use"], answers=["2"])
assert rc == 0 and state()["campaign"] == "test_doraemon_2026_smoke_v0.0", (out, state())
rc, out, err, _ = run(["use", "test_doraemon_2026_v0.1"])
assert rc == 0 and state()["campaign"] == "test_doraemon_2026_v0.1"
rc, out, err, _ = run(["use", "nope"])
assert rc == 1 and "no campaign nope" in err
print("OK: `dprod use` switches by menu or by name")

# no terminal and nothing remembered: a clear error listing the choices
os.remove(STATE)
rc, out, err, prompts = run(["status", "--no-sync"], tty=False)
assert rc == 1 and not prompts and "test_doraemon_2026_smoke_v0.0" in err and "test_doraemon_2026_v0.1" in err, err
print("OK: non-interactive without a choice -> error listing the campaigns: %s" % err.strip())
print("SELECT TEST OK")
