"""Installation test: run one small job through 1 -> 2A -> 3A -> 2B -> 3B.

    bin/dprod-install-test --site s3df [--image IMG | --image1 .. --image2 .. --image3 ..]
                           [--events 5] [--outdir DIR] [--stages 1,2A,3A,2B,3B]

Runs on a (GPU) node directly, without slurm: it creates a throwaway 1-job
campaign from the real campaign config and runs every stage through the
production worker, in the stage's container, one after another. So it tests
exactly what production runs -- commands, containers, environment, id
validation, provenance, resource monitoring -- only the scheduler is local.

Containers: by default each stage uses the image the site config assigns it.
--image sets one image for all stages; --image1/2/3 override stage 1, stages
2A/2B (JAXTPC) and stages 3A/3B (pysupera) separately (they win over --image).

Output (--outdir, default ./install_test_<date>):
    1_edepsim/  2A_jaxtpc_wire/  3A_supera_wire/  2B_jaxtpc_pixel/  3B_supera_pixel/
        outputs (symlinks into campaign/data/...), logs/ (the job's log tarball, unpacked),
        slurm.log (the job's stdout)
    campaign/      the throwaway campaign (bookkeeping db, summaries, data)
    REPORT.txt     per stage: status, time, memory, GPU, files, events, id checks,
                   provenance chain
"""

import argparse
import glob
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import time

ORDER = ("1", "2A", "3A", "2B", "3B")


def _say(msg):
    print("[install-test %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# --------------------------------------------------------------------------- controller side

def run(a):
    import yaml
    from . import config as C
    from . import db as D
    from . import layout as L
    from .campaign import Campaign
    from .report import print_status

    outdir = os.path.abspath(a.outdir or "install_test_%s" % time.strftime("%Y%m%d_%H%M%S"))
    if os.path.exists(outdir) and os.listdir(outdir):
        raise SystemExit("output directory %s exists and is not empty" % outdir)
    os.makedirs(outdir, exist_ok=True)

    # ---- site: the real one, with a local scheduler and paths under outdir
    site = C.load_site(a.site, check_paths=False)   # storage/log paths are replaced below
    site["scheduler"] = "local"
    site["storage_root"] = os.path.join(outdir, "campaign")
    site["log_root"] = os.path.join(outdir, "joblog")
    site["db_path"] = None
    if a.work_root:
        site["work_root"] = a.work_root
    elif "$" in os.path.expandvars(site["work_root"]):
        site["work_root"] = os.path.join(outdir, "work")   # e.g. $LSCRATCH outside a job
    if a.container_exec is not None:
        site["container"]["exec"] = a.container_exec
        site["container"]["exec_login"] = a.container_exec
    if a.gpu_flags is not None:
        site["slurm"]["profiles"].setdefault("gpu", {})["container_flags"] = a.gpu_flags

    # ---- campaign: the real config, one job, images per stage group
    raw = yaml.safe_load(open(a.config))
    tag = "install_test"
    raw["campaign"] = tag
    raw["max_attempts"] = 1
    groups = {"1": a.image1, "2": a.image2, "3": a.image3}
    if raw.get("inherit"):
        raise SystemExit("%s inherits stages from campaign %s; the installation test needs a config "
                         "that defines every stage (e.g. the source campaign's)" % (
                             a.config, raw["inherit"].get("campaign")))
    cfg0 = C.normalize_campaign(raw, a.config)
    want = [C.resolve_stage(cfg0, x) for x in a.stages.split(",")]
    for name, s in raw["stages"].items():
        s.pop("max_queued", None)
        alias = str(s.get("alias", ""))
        img = groups.get(alias[:1]) or a.image
        if img:
            key = "_install_%s" % alias[:1]
            site["images"][key] = os.path.abspath(img) if os.path.exists(img) else img
            s["image"] = key
        if name == cfg0["root_stage"]:
            s["n_jobs"] = 1
            s["events_per_job"] = a.events
            s["input_dir"] = os.path.join(C.REPO_DIR, s["input_dir"]) \
                if not os.path.isabs(s["input_dir"]) else s["input_dir"]
        s["enabled"] = name in want
        if s.get("profile") == "gpu" and s.get("monitor_gpu") is None:
            s["monitor_gpu"] = True      # GPU stages: always sample the GPU here
    cfg_path = os.path.join(outdir, "install_test_campaign.yaml")
    with open(cfg_path, "w") as f:
        try:
            yaml.safe_dump(raw, f, sort_keys=False)
        except TypeError:                # PyYAML < 5.1
            yaml.safe_dump(raw, f)

    _say("output directory %s" % outdir)
    c = Campaign.init(site, cfg_path)
    cfg = c.cfg
    img_of = {n: (site["images"].get(s["image"] or "", "") or "(none)") for n, s in cfg["stages"].items()}
    for n in want:
        _say("  %-14s [%s] image %s" % (n, cfg["stages"][n]["alias"], img_of[n]))

    # ---- run the stages in the requested order
    results = {}
    order = sorted(want, key=lambda n: ORDER.index(cfg["stages"][n]["alias"])
                   if cfg["stages"][n]["alias"] in ORDER else 99)
    for name in order:
        s = cfg["stages"][name]
        parent = s["parent"]
        if parent and results.get(parent) != "done":
            _say("%s: skipped (parent %s %s)" % (name, parent, results.get(parent, "not run")))
            results[name] = "skipped"
            continue
        _say("%s [%s]: running ..." % (name, s["alias"]))
        t0 = time.time()
        c.advance([name], out=lambda m: None)
        c.sync()
        rows = D.tasks(c.con, name)
        st = sorted(set(r["status"] for r in rows))
        results[name] = "done" if st == ["done"] else ",".join(st)
        _say("%s [%s]: %s in %.0f s" % (name, s["alias"], results[name], time.time() - t0))
        if results[name] != "done":
            for r in rows:
                if r["note"]:
                    note = [ln for ln in r["note"].splitlines() if ln.strip()]
                    _say("  task %d: %s" % (r["task_id"], note[0]))
                    for ln in note[1:12]:
                        print("      | " + ln)

    # ---- organize per stage
    for name in order:
        s = cfg["stages"][name]
        d = os.path.join(outdir, "%s_%s" % (s["alias"], name))
        os.makedirs(d, exist_ok=True)
        for t in D.tasks(c.con, name):
            tname = L.task_name(name, t["first_job"], t["last_job"])
            for f in D.task_files(c.con, name, t["task_id"]):
                src = os.path.join(c.dir, f["path"])
                rel = f["path"].split(tname + "/", 1)[1] if tname + "/" in f["path"] \
                    else os.path.basename(f["path"])
                dst = os.path.join(d, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                os.symlink(src, dst)
            for att in range(1, t["n_attempts"] + 1):
                tgz = os.path.join(c.dir, L.logs_rel_path(name, tname, att))
                if os.path.exists(tgz):
                    with tarfile.open(tgz) as tf:
                        try:        # python >= 3.12 (and patched 3.9+): safe extraction filter
                            tf.extractall(os.path.join(d, "logs"), filter="data")
                        except TypeError:
                            tf.extractall(os.path.join(d, "logs"))
            for log in glob.glob(os.path.join(site["log_root"], tag, name, "*.out")):
                shutil.copy(log, os.path.join(d, "slurm.log"))

    # ---- report: bookkeeping (here) + file checks (in a container with h5py)
    lines = ["DORAEMON installation test  %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
             "site %s   host %s   events per job %d" % (site["name"], os.uname().nodename, a.events),
             "campaign config %s" % os.path.abspath(a.config), ""]
    for name in order:
        s = cfg["stages"][name]
        lines.append("%-4s %-14s %-9s image %s" % (s["alias"], name, results[name], img_of[name]))
        for t in D.tasks(c.con, name):
            at = c.con.execute("SELECT * FROM attempts WHERE stage = ? AND task_id = ? AND attempt = ?",
                               (name, t["task_id"], t["n_attempts"])).fetchone()
            if at is None:
                continue
            gpu = ("  GPU %.0f%% / %.0f MB" % (at["gpu_util_pct"], at["gpu_mem_used_mb"] or 0)
                   if at["gpu_util_pct"] is not None else "")
            lines.append("       wall %s s  RAM avg/max %s/%s MB%s" % (
                "%.0f" % at["wall_s"] if at["wall_s"] else "-",
                "%.0f" % at["avg_rss_mb"] if at["avg_rss_mb"] else "-",
                "%.0f" % at["max_rss_mb"] if at["max_rss_mb"] else "-", gpu))
            if at["reason"]:
                # the reason carries the tail of the command's log: show it, it says why
                rl = [ln for ln in at["reason"].splitlines() if ln.strip()]
                lines.append("       reason: " + rl[0])
                lines.extend("         | " + ln for ln in rl[1:25])
    lines.append("")
    report = os.path.join(outdir, "REPORT.txt")
    with open(report, "w") as f:
        f.write("\n".join(lines) + "\n")

    stage_imgs = [img_of[n] for n in order if img_of[n] != "(none)"]
    prefix = C.container_prefix(site, dict(cfg["stages"][order[-1]], container_flags=""),
                                login=True) if stage_imgs else ""
    cmd = "%s env PYTHONPATH=%s python3 -m doraemon_prod.install_test --check %s --report %s" % (
        prefix, shlex.quote(C.REPO_DIR), shlex.quote(outdir), shlex.quote(report))
    rc = subprocess.call(cmd.strip(), shell=True)
    with open(report, "a") as f:
        f.write("\nStatus table\n")
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            print_status(c)
        f.write(buf.getvalue())
    print(open(report).read())
    ok = all(results[n] == "done" for n in order) and rc == 0
    _say("%s -- report: %s" % ("PASSED" if ok else "FAILED", report))
    return 0 if ok else 1


# --------------------------------------------------------------------------- container side

def check(outdir, report):
    """File-level validation; runs where h5py is available (inside a stage image)."""
    import h5py
    from . import db as D
    from .idreaders import read_ids
    from .provenance import read_provenance, task_blocks

    cdir = os.path.join(outdir, "campaign", "install_test")
    con = D.connect(os.path.join(cdir, "bookkeeping.sqlite"))
    import yaml
    cfg = yaml.safe_load(open(os.path.join(cdir, "campaign.yaml")))
    root = [n for n, s in cfg["stages"].items() if not s.get("parent")][0]
    n_ev = int(cfg["stages"][root]["events_per_job"])
    chain = {}
    for n, s in cfg["stages"].items():
        chain[n] = (chain.get(s.get("parent"), []) if s.get("parent") else []) + [n]
    lines, bad = ["File checks"], 0
    for name, s in cfg["stages"].items():
        files = con.execute("SELECT * FROM files WHERE stage = ? ORDER BY role, path", (name,)).fetchall()
        if not files:
            continue
        lines.append("  %s [%s]" % (name, s.get("alias")))
        for f in files:
            path = os.path.join(cdir, f["path"])
            msgs = []
            try:
                reader = "edepsim" if name == root else s.get("id_reader")
                ids = read_ids(reader, path, os.path.basename(path), s.get("id_reader_options"))
                nev = sum(len(v) for v in ids.values())
                msgs.append("jobs %s, %d event(s)%s" % (sorted(ids), nev,
                            "" if nev == n_ev else " (expected %d)" % n_ev))
                if nev != n_ev:
                    bad += 1
            except Exception as e:
                msgs.append("ID CHECK FAILED: %s" % e)
                bad += 1
            prov = read_provenance(path)
            if prov is not None:
                got = prov.get("stages")
                okc = got == chain[name]
                msgs.append("provenance %s%s" % ("->".join(got or []), "" if okc else
                                                 " (EXPECTED %s)" % "->".join(chain[name])))
                if not okc:
                    bad += 1
                sw = []
                for st in got or []:
                    for b in task_blocks(prov, st):
                        for k, v in (b.get("software") or {}).items():
                            if isinstance(v, dict) and v.get("git_describe"):
                                sw.append("%s=%s%s" % (k, v["git_describe"],
                                                       "(dirty)" if v.get("git_dirty") is True else ""))
                if sw:
                    msgs.append("software " + " ".join(sorted(set(sw))))
            size = os.path.getsize(path) / 1e6
            lines.append("    %-9s %-44s %8.1f MB  %s" % (f["role"], os.path.basename(path), size,
                                                          "; ".join(msgs)))
    lines.append("  %s" % ("all file checks passed" if not bad else "%d PROBLEM(S)" % bad))
    with open(report, "a") as fh:
        fh.write("\n".join(lines) + "\n")
    return 0 if not bad else 1


def main(argv=None):
    ap = argparse.ArgumentParser(prog="dprod-install-test", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", metavar="OUTDIR", help=argparse.SUPPRESS)
    ap.add_argument("--report", help=argparse.SUPPRESS)
    ap.add_argument("--site", default=os.environ.get("DPROD_SITE"),
                    help="site name or path (default: the current / detected site, see `dprod sites`)")
    ap.add_argument("--config", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "configs", "campaigns", "test_doraemon_2026_v0.1.yaml"),
        help="campaign config whose stage definitions are tested")
    ap.add_argument("--image", help="one container image for all stages")
    ap.add_argument("--image1", help="image for stage 1 (edep-sim)")
    ap.add_argument("--image2", help="image for stages 2A/2B (JAXTPC)")
    ap.add_argument("--image3", help="image for stages 3A/3B (pysupera)")
    ap.add_argument("--events", type=int, default=5, help="events to generate (5)")
    ap.add_argument("--stages", default=",".join(ORDER), help="stages to run (1,2A,3A,2B,3B)")
    ap.add_argument("--outdir", help="output directory (default ./install_test_<date>)")
    ap.add_argument("--work-root", help="scratch for the jobs (default: site work_root, "
                    "or <outdir>/work if that is not defined here)")
    ap.add_argument("--container-exec", help="override the site's container command template, "
                    "e.g. 'apptainer exec {flags} -B /home {image}'")
    ap.add_argument("--gpu-flags", help="container flags for GPU stages (site default, e.g. --nv)")
    a = ap.parse_args(argv)
    if a.check:
        return check(a.check, a.report)
    if not a.site:
        from .select import SelectError, pick_site
        try:
            a.site = pick_site(None, note=lambda m: print("dprod-install-test: " + m))
        except SelectError as e:
            ap.error(str(e))
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
