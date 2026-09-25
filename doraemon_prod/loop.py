"""Self-resubmitting controller chain: automated rounds without scrontab/cron.

    bin/dprod-cron --loop 15m [--name N] [--partition P --account A --qos Q --time T]
                   --site s3df CAMPAIGN [...] [-- extra `dprod watch` options]
    bin/dprod-cron --stop   [--name N] --site s3df
    bin/dprod-cron --status [--name N] --site s3df

The round jobs use the site's `slurm.profiles.cron` only (not slurm.default nor the
cpu profile, so e.g. a preemptable default QOS does not apply), on top of 1 CPU,
4 GB, 30 min; --partition/--account/--qos/--time override it. The campaigns must
exist (`dprod init`).

`--loop` writes a small batch script and submits it. Each job of the chain first
submits the next one (sbatch --begin=now+INTERVAL; so a failing round does not end
the chain), then runs one `dprod-cron` round (sync, submit what is ready, retries,
monitoring page, summaries). A job does not resubmit if another job of the chain
is already queued, or after `--stop` (stop file + scancel).

Files: <log_root>/cron/<name>.sh (the job script), <name>.log (appended by every
round), <name>.stop (while stopped).

`resubmit` (used inside the job) needs only the standard library.
"""

import argparse
import os
import re
import shlex
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def parse_interval(text):
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", str(text))
    if not m:
        raise ValueError("interval %r: use e.g. 900, 30s, 15m, 1h" % text)
    return int(float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)])


def _run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if p.returncode != 0:
        raise RuntimeError("%s failed: %s" % (" ".join(cmd), p.stderr.strip()))
    return p.stdout


def queued(name, exclude=None):
    """Job ids of this user's jobs with that name that are pending or running."""
    out = _run(["squeue", "-h", "-u", os.environ.get("USER", ""), "-n", name, "-o", "%i %T"])
    ids = []
    for line in out.splitlines():
        f = line.split()
        if f and f[0] != str(exclude):
            ids.append((f[0], f[1] if len(f) > 1 else ""))
    return ids


def paths(cron_dir, name):
    return (os.path.join(cron_dir, name + ".sh"), os.path.join(cron_dir, name + ".log"),
            os.path.join(cron_dir, name + ".stop"))


def submit(script, delay_s=0):
    cmd = ["sbatch", "--parsable"]
    if delay_s:
        cmd.append("--begin=now+%d" % int(delay_s))
    return _run(cmd + [script]).strip().split(";")[0]


# --------------------------------------------------------------------------- inside the job

def resubmit(a):
    """First thing in every round job: queue the next round (unless stopped/duplicate)."""
    me = os.environ.get("SLURM_JOB_ID")
    if os.path.exists(a.stopfile):
        print("dprod-loop: stop file %s present, not resubmitting (chain ends)" % a.stopfile)
        return 0
    others = [j for j, st in queued(a.name, exclude=me) if st.upper().startswith("PEND")]
    if others:
        print("dprod-loop: next round already queued (%s), not resubmitting" % ",".join(others))
        return 0
    jid = submit(a.script, a.interval)
    print("dprod-loop: next round submitted as job %s (starts in %d s)" % (jid, a.interval))
    return 0


# --------------------------------------------------------------------------- controller side

def _cron_dir(site):
    return os.path.join(site["log_root"], "cron")


def start(a):
    from . import config as C
    site = C.load_site(a.site)
    interval = parse_interval(a.loop)
    if interval < 60:
        raise SystemExit("dprod-loop: interval must be at least 60 s")
    cron_dir = _cron_dir(site)
    os.makedirs(cron_dir, exist_ok=True)
    script, log, stopfile = paths(cron_dir, a.name)
    running = queued(a.name)
    if running:
        raise SystemExit("dprod-loop: chain %s is already active (jobs %s); `dprod-cron --stop` first"
                         % (a.name, ", ".join("%s %s" % j for j in running)))
    for tag in a.campaigns:
        if not os.path.exists(os.path.join(site["storage_root"], tag, "campaign.yaml")):
            raise SystemExit("dprod-loop: campaign %s is not initialized at site %s; run `bin/dprod init` "
                             "first (see `bin/dprod campaigns`)" % (tag, site["name"]))
    # the round jobs use their own profile only -- not slurm.default (e.g. a preemptable
    # QOS) and not the cpu profile of the production jobs
    prof = (site["slurm"].get("profiles") or {}).get("cron")
    if prof is None:
        raise SystemExit(
            "dprod-loop: site %s has no slurm profile 'cron' for the controller rounds. Add e.g.\n"
            "slurm:\n  profiles:\n    cron:               # dprod-cron --loop round jobs (not preemptable)\n"
            "      partition: milano\n      account: mli:nu-ml-dev\n      qos: <non-preemptable QOS>\n"
            "to %s (or pass --partition/--account/--qos)" % (site["name"], site["_path"]))
    opts = {"nodes": 1, "ntasks": 1, "cpus_per_task": 1, "mem": "4G", "time": "00:30:00"}
    opts.update(prof or {})
    opts.pop("container_flags", None)
    opts.pop("extra", None)
    for k in ("partition", "account", "qos", "time"):
        if getattr(a, k):
            opts[k] = getattr(a, k)
    lines = ["#!/bin/bash",
             "# dprod controller chain %r, written by `dprod-cron --loop` on %s." % (a.name, time.strftime("%Y-%m-%d %H:%M")),
             "# Every job first queues the next round, then runs one round. Stop: dprod-cron --stop --name %s" % a.name,
             "#SBATCH --job-name=%s" % a.name,
             "#SBATCH --output=%s" % log, "#SBATCH --open-mode=append"]
    for k, v in opts.items():
        if v is None or v is False or v == "":
            continue
        lines.append("#SBATCH --%s=%s" % (k.replace("_", "-"), v))
    cron_cmd = [os.path.join(REPO, "bin", "dprod-cron"), "--site", a.site] + a.campaigns
    if a.extra:
        cron_cmd += ["--"] + a.extra
    lines += [
        "",
        'echo "=== dprod-loop %s: job ${SLURM_JOB_ID} on $(hostname) at $(date)"' % a.name,
        "env PYTHONPATH=%s python3 -m doraemon_prod.loop resubmit --script %s --name %s --interval %d --stopfile %s" % (
            shlex.quote(REPO), shlex.quote(script), shlex.quote(a.name), interval, shlex.quote(stopfile)),
        " ".join(shlex.quote(x) for x in cron_cmd),
        "",
    ]
    with open(script, "w") as f:
        f.write("\n".join(lines))
    os.chmod(script, 0o755)
    if os.path.exists(stopfile):
        os.remove(stopfile)
    jid = submit(script)
    print("chain %s started: first round is job %s, then every %s" % (a.name, jid, a.loop))
    print("  campaigns: %s" % " ".join(a.campaigns))
    print("  script:    %s" % script)
    print("  log:       %s" % log)
    print("  stop with: bin/dprod-cron --stop --name %s --site %s" % (a.name, a.site))
    return 0


def stop(a):
    from . import config as C
    site = C.load_site(a.site)
    script, log, stopfile = paths(_cron_dir(site), a.name)
    os.makedirs(os.path.dirname(stopfile), exist_ok=True)
    open(stopfile, "w").write("stopped %s\n" % time.strftime("%Y-%m-%d %H:%M:%S"))
    jobs = queued(a.name)
    if jobs:
        _run(["scancel"] + [j for j, _ in jobs])
    print("chain %s stopped (cancelled %d job(s): %s)" % (a.name, len(jobs), ", ".join(j for j, _ in jobs) or "-"))
    return 0


def status(a):
    from . import config as C
    site = C.load_site(a.site)
    script, log, stopfile = paths(_cron_dir(site), a.name)
    jobs = queued(a.name)
    if os.path.exists(stopfile):
        print("chain %s: stopped (%s)" % (a.name, open(stopfile).read().strip()))
    elif not jobs:
        print("chain %s: not active (no queued or running round)" % a.name)
    for j, st in jobs:
        print("chain %s: job %s %s" % (a.name, j, st))
    if os.path.exists(log):
        print("--- last lines of %s" % log)
        with open(log, errors="replace") as f:
            print("".join(f.readlines()[-a.lines:]).rstrip())
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv and argv[0] == "resubmit":
        ap = argparse.ArgumentParser(prog="doraemon_prod.loop resubmit")
        ap.add_argument("--script", required=True)
        ap.add_argument("--name", required=True)
        ap.add_argument("--interval", type=int, required=True)
        ap.add_argument("--stopfile", required=True)
        try:
            return resubmit(ap.parse_args(argv[1:]))
        except Exception as e:           # never let this stop the round itself
            print("dprod-loop: could not queue the next round: %s" % e)
            return 0
    extra = []
    if "--" in argv:
        i = argv.index("--")
        argv, extra = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(prog="dprod-cron", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--loop", metavar="INTERVAL", help="start a chain: a round every INTERVAL (e.g. 15m)")
    g.add_argument("--stop", action="store_true", help="stop the chain")
    g.add_argument("--status", action="store_true", help="show the chain's jobs and log tail")
    ap.add_argument("--site", default=os.environ.get("DPROD_SITE"))
    ap.add_argument("--name", default="dprod-cron", help="chain name = slurm job name (default dprod-cron)")
    ap.add_argument("--partition")
    ap.add_argument("--account")
    ap.add_argument("--qos", help="override the site's slurm.profiles.cron qos")
    ap.add_argument("--time", help="time limit of one round (default: cron profile, else 00:30:00)")
    ap.add_argument("--lines", type=int, default=25, help="--status: log lines to show")
    ap.add_argument("campaigns", nargs="*")
    a = ap.parse_args(argv)
    a.extra = extra
    if not a.site:
        from .select import SelectError, pick_site
        try:
            a.site = pick_site(None)
        except SelectError as e:
            ap.error(str(e))
    if a.loop:
        if not a.campaigns:
            ap.error("--loop needs the campaign(s) to run rounds for")
        return start(a)
    return stop(a) if a.stop else status(a)


if __name__ == "__main__":
    sys.exit(main())
