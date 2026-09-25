"""Test the self-resubmitting controller chain (dprod-cron --loop / --status / --stop)
with fake sbatch/squeue/scancel. Run inside the stage-1 image:
  apptainer exec -B /home/kazu,/tmp <larcv2 image> python3 tests/test_loop.py <scratch dir>
"""
import json
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.join(HERE, "..")
sys.path.insert(0, REPO)
from doraemon_prod import config as C, db as D  # noqa: E402
from doraemon_prod.campaign import Campaign  # noqa: E402

ROOT = os.path.abspath(sys.argv[1])
if os.path.exists(ROOT):
    shutil.rmtree(ROOT)
os.makedirs(os.path.join(ROOT, "fail"))
env = dict(os.environ, DPROD_LOCAL_ROOT=ROOT, DPROD_STATE="", DPROD_BATCH="1",
           FAKE_SLURM_DIR=os.path.join(ROOT, "slurm"),
           PATH=os.path.join(HERE, "fake_slurm") + ":" + os.environ["PATH"], USER="tester")
os.environ.update(env)
site = os.path.join(ROOT, "site.yaml")
text = open(os.path.join(REPO, "configs", "sites", "local.yaml")).read()
text = text.replace("vars:", "vars:\n  test_dir: %s\n  fail_dir: %s" % (HERE, os.path.join(ROOT, "fail")), 1)
# a preemptable default QOS (like S3DF's) that the controller rounds must not inherit
text = text.replace("  default: {}", "  default:\n    qos: preemptable", 1)
text = text.replace("    cron: {}", "    cron:\n      partition: roma\n      qos: normal\n      time: \"00:20:00\"", 1)
open(site, "w").write(text)
nocron = os.path.join(ROOT, "site_nocron.yaml")
open(nocron, "w").write(text.replace("    cron:\n      partition: roma\n      qos: normal\n      time: \"00:20:00\"\n", ""))
cfg = os.path.join(ROOT, "c.yaml")
open(cfg, "w").write(open(os.path.join(HERE, "campaign_local_test.yaml")).read().replace(
    "campaign: localtest_doraemon_v0.0", "campaign: looptest").replace("n_jobs: 6", "n_jobs: 2").replace(
    "events_per_job: 3", "events_per_job: 2").replace(" --gpu-seconds 8", "").replace(
    "monitor_gpu: true", "monitor_gpu: false"))
CRON = os.path.join(REPO, "bin", "dprod-cron")


def sh(*args, rc=0):
    p = subprocess.run(list(args), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, env=env)
    print("+ %s\n%s" % (" ".join(os.path.basename(a) if a.startswith("/") else a for a in args), p.stdout.rstrip()))
    assert p.returncode == rc, (p.returncode, rc)
    return p.stdout


def queue():
    return json.load(open(os.path.join(ROOT, "slurm", "queue.json")))["jobs"]


def pending():
    return [j for j, x in sorted(queue().items()) if x["state"] == "PENDING"]


def run_next():
    """Run the oldest pending job like slurm would."""
    jid = pending()[0]
    q = json.load(open(os.path.join(ROOT, "slurm", "queue.json")))
    q["jobs"][jid]["state"] = "RUNNING"
    json.dump(q, open(os.path.join(ROOT, "slurm", "queue.json"), "w"))
    script_text = open(q["jobs"][jid]["script"]).read()
    logf = re.search(r"^#SBATCH --output=(\S+)", script_text, re.M).group(1)   # like slurm, append
    with open(logf, "a") as lf:
        rc = subprocess.call(["bash", q["jobs"][jid]["script"]], env=dict(env, SLURM_JOB_ID=jid),
                             stdout=lf, stderr=subprocess.STDOUT)
    q = json.load(open(os.path.join(ROOT, "slurm", "queue.json")))
    q["jobs"][jid]["state"] = "COMPLETED" if rc == 0 else "FAILED"
    json.dump(q, open(os.path.join(ROOT, "slurm", "queue.json"), "w"))
    return jid, rc


def statuses(stage):
    c = Campaign(C.load_site(site), "looptest")
    c.sync()
    return [t["status"] for t in D.tasks(c.con, stage)]


sh(CRON, "--loop", "15m", "--site", site, "looptest", rc=1)           # not initialized yet
sh(os.path.join(REPO, "bin", "dprod"), "--site", site, "init", cfg)
out = sh(CRON, "--loop", "15m", "--site", nocron, "looptest", rc=1)   # no cron profile
assert "profiles:" in out and "cron:" in out
print("OK: --loop refuses an uninitialized campaign and a site without a cron profile")
sh(CRON, "--loop", "15m", "--site", site, "--account", "neutrino:ml-dev", "looptest", "--", "--max-queued", "50")
assert len(pending()) == 1
script = queue()[pending()[0]]["script"]
body = open(script).read()
assert "#SBATCH --job-name=dprod-cron" in body and "#SBATCH --account=neutrino:ml-dev" in body
assert "#SBATCH --qos=normal" in body and "preemptable" not in body and "#SBATCH --partition=roma" in body
assert "#SBATCH --time=00:20:00" in body and "#SBATCH --mem=4G" in body
assert "--max-queued 50" in body and "--open-mode=append" in body
print("OK: round jobs use the cron profile only (qos normal, not the preemptable default) + account override")

sh(CRON, "--loop", "15m", "--site", site, "looptest", rc=1)       # a second chain is refused
print("OK: a second chain with the same name is refused")

j1, rc = run_next()                                               # round 1: stage 1
assert rc == 0 and len(pending()) == 1 and queue()[pending()[0]]["begin"] == "now+900"
assert statuses("edepsim") == ["done", "done"]
j2, rc = run_next()                                               # round 2: stage 2A
assert rc == 0 and len(pending()) == 1
assert statuses("jaxtpc_wire") == ["done"]
print("OK: every round queued the next one (--begin=now+900) and advanced the campaign")

log = os.path.join(ROOT, "joblog", "cron", "dprod-cron.log")
assert open(log).read().count("=== dprod-loop dprod-cron: job") == 2
out = sh(CRON, "--status", "--site", site)
assert "PENDING" in out and "last lines of" in out and "round 1" in out

# duplicate guard: a round that finds the next one already queued does not add another
subprocess.call(["bash", script], env=dict(env, SLURM_JOB_ID="999"), stdout=subprocess.DEVNULL)
assert len(pending()) == 1
print("OK: no duplicate next round when one is already queued")

sh(CRON, "--stop", "--site", site)
assert not pending()
out = subprocess.run(["bash", script], env=dict(env, SLURM_JOB_ID="998"), stdout=subprocess.PIPE,
                     universal_newlines=True).stdout
assert not pending() and "stop file" in out, out
out = sh(CRON, "--status", "--site", site)
assert "stopped" in out
print("OK: --stop cancelled the queued round; a round already running does not resubmit")
sh(CRON, "--loop", "30m", "--site", site, "looptest")
assert len(pending()) == 1 and not os.path.exists(os.path.join(ROOT, "joblog", "cron", "dprod-cron.stop"))
print("OK: the chain can be started again after a stop")
print("LOOP TEST OK")
