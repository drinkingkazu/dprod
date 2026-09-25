#!/usr/bin/env python3
"""Minimal sbatch / squeue / scancel stand-ins for tests (queue kept in $FAKE_SLURM_DIR)."""
import json
import os
import re
import sys

D = os.environ["FAKE_SLURM_DIR"]
os.makedirs(D, exist_ok=True)
Q = os.path.join(D, "queue.json")


def load():
    return json.load(open(Q)) if os.path.exists(Q) else {"next": 1000, "jobs": {}}


def save(q):
    json.dump(q, open(Q, "w"), indent=1)


cmd = os.path.basename(sys.argv[0])
args = sys.argv[1:]
q = load()
if cmd == "sbatch":
    begin = [a for a in args if a.startswith("--begin=")]
    script = [a for a in args if not a.startswith("--")][-1]
    m = re.search(r"^#SBATCH --job-name=(\S+)", open(script).read(), re.M)
    jid = str(q["next"]); q["next"] += 1
    q["jobs"][jid] = {"name": m.group(1) if m else "x", "state": "PENDING", "script": script,
                      "begin": begin[0].split("=", 1)[1] if begin else "now"}
    save(q)
    print(jid)
elif cmd == "squeue":
    name = args[args.index("-n") + 1] if "-n" in args else None
    for jid, j in sorted(q["jobs"].items()):
        if j["state"] in ("PENDING", "RUNNING") and (name is None or j["name"] == name):
            print("%s %s" % (jid, j["state"]))
elif cmd == "scancel":
    for jid in args:
        if jid in q["jobs"]:
            q["jobs"][jid]["state"] = "CANCELLED"
    save(q)
