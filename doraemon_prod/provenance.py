"""Provenance: embed the configuration that produced a file into the file itself.

Worker side (h5py; PyYAML optional). Each stage appends its own block to the
top-level /provenance group, and a downstream stage first copies the
/provenance of the input file(s) the output was derived from, so a file carries
the full chain back to the generator:

  /provenance                   @campaign  @format_version
                                @stages     stage names in processing order
    campaign_config/            the campaign config, once per file
      campaign.yaml             [text]   (@path, @sha256)
      config/                   the same as a dictionary
    campaign_config_history/<sha16>/   earlier versions an input was produced with
                                (after `dprod update-config`); every task block
                                names its version in @campaign_config_sha256
    <stage>/                    one block per stage, e.g. edepsim/, jaxtpc_wire/
                                @stage @alias @handler
      stage_config/             options set for this stage in the campaign config
      j<first>-<last>/          one task block per task of this stage that
                                contributed to the file (the job range it processed):
                                @task_id @attempt @first_job @last_job @host
                                @slurm_job_id @created @image @command @dprod_version
        seeds/                  @task [, @geant4, @generator]
        files/<name>            [text] config file as used (e.g. geometry, macro
                                as executed); @<name>.path, @<name>.sha256 on files/
        config/<name>/          the same file as a dictionary, if YAML/JSON
        software/<name>/        @path, @git_commit/@git_dirty/@git_describe/@git_remote,
                                @sha256 for executables
        container/              the image the job ran in: @runtime @path @size @mtime_iso
                                @head_tail_sha256 (size + first/last 4 MiB) @build_date
                                @definition_sha256, labels/ (the image labels); the
                                definition file itself is files/container_definition
        environment/            environment variables set for the stage
        inputs                  [string array] input file names
    _blobs/<sha256>             storage of the texts; every files/<name> is a hard
                                link to its blob, so identical texts (e.g. the
                                geometry in N merged jobs) are stored once

Dictionary encoding (dict_to_group / group_to_dict): dict -> group; scalar ->
attribute; list of numbers/strings -> array attribute; list of dicts ->
subgroup with children "0", "1", ... (@_list=1); None -> empty attribute;
anything else -> JSON string attribute (named in @_json). The raw text is
always stored as well.

Read back: read_provenance(path) -> nested dict, task_blocks(prov, stage) ->
list of task blocks, or `python3 -m doraemon_prod.provenance <file> [--files]`.
"""

import hashlib
import json
import os
import subprocess
import sys
import time

import h5py
import numpy as np

from . import __version__

GROUP = "provenance"
BLOBS = "_blobs"
FORMAT_VERSION = 2
STR = h5py.string_dtype()


class ProvenanceError(Exception):
    pass


# --------------------------------------------------------------------------- dict <-> group

def _is_scalar(v):
    return isinstance(v, (str, bool, int, float, np.integer, np.floating))


def dict_to_group(grp, d):
    json_keys = []
    for k, v in d.items():
        key = str(k).replace("/", "|")
        if isinstance(v, dict):
            dict_to_group(grp.create_group(key), v)
        elif v is None:
            grp.attrs[key] = h5py.Empty("f")
        elif _is_scalar(v):
            grp.attrs[key] = v
        elif isinstance(v, (list, tuple)) and v and all(isinstance(x, dict) for x in v):
            sub = grp.create_group(key)
            sub.attrs["_list"] = 1
            for i, x in enumerate(v):
                dict_to_group(sub.create_group(str(i)), x)
        elif isinstance(v, (list, tuple)) and all(isinstance(x, str) for x in v):
            grp.attrs.create(key, np.array(list(v), dtype=object), dtype=STR)
        elif (isinstance(v, (list, tuple)) and v and
              all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in v)):
            grp.attrs[key] = np.array(v)
        else:
            grp.attrs[key] = json.dumps(v, default=str)
            json_keys.append(key)
    if json_keys:
        grp.attrs.create("_json", np.array(json_keys, dtype=object), dtype=STR)


def _py(v):
    if isinstance(v, h5py.Empty):
        return None
    if isinstance(v, bytes):
        return v.decode()
    if isinstance(v, np.ndarray):
        return [_py(x) for x in v.tolist()]
    if isinstance(v, np.generic):
        return v.item()
    return v


def group_to_dict(grp):
    json_keys = set(_py(grp.attrs["_json"])) if "_json" in grp.attrs else set()
    if grp.attrs.get("_list"):
        return [group_to_dict(grp[str(i)]) for i in range(len(grp))]
    out = {}
    for k, v in grp.attrs.items():
        if k in ("_json", "_list"):
            continue
        out[k] = json.loads(_py(v)) if k in json_keys else _py(v)
    for k, sub in grp.items():
        if k == BLOBS:
            continue
        out[k] = group_to_dict(sub) if isinstance(sub, h5py.Group) else _py(sub[()])
    return out


# --------------------------------------------------------------------------- collection

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text):
    return hashlib.sha256(text.encode()).hexdigest()


def git_info(path):
    """Commit / dirty state / describe of a git checkout (empty dict if not a repo)."""
    def git(*args):
        p = subprocess.run(["git", "-C", path] + list(args), stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, universal_newlines=True, timeout=60)
        return p.stdout.strip() if p.returncode == 0 else None
    try:
        commit = git("rev-parse", "HEAD")
    except (OSError, subprocess.TimeoutExpired):
        return {}
    if not commit:
        return {}
    status = git("status", "--porcelain", "--untracked-files=no")
    return {"git_commit": commit, "git_dirty": bool(status) if status is not None else "unknown",
            "git_describe": git("describe", "--always", "--dirty", "--tags") or "",
            "git_remote": git("config", "--get", "remote.origin.url") or ""}


def software_info(path):
    """git info for a directory; sha256 (+ git info of its directory) for a file."""
    info = {"path": path}
    if os.path.isdir(path):
        info.update(git_info(path))
    elif os.path.isfile(path):
        info["sha256"] = sha256_file(path)
        info.update(git_info(os.path.dirname(path)))
    else:
        info["missing"] = True
    return info


def _head_tail_sha256(path, chunk=4 << 20):
    """SHA-256 of size + first and last `chunk` bytes: a cheap fingerprint of a large
    file (a rebuilt image differs even if its modification time was preserved)."""
    size = os.path.getsize(path)
    h = hashlib.sha256(str(size).encode())
    with open(path, "rb") as f:
        h.update(f.read(chunk))
        if size > chunk:
            f.seek(max(chunk, size - chunk))
            h.update(f.read(chunk))
    return h.hexdigest()


SINGULARITY_D = "/.singularity.d"


def container_info(image_hint=None):
    """What container this job runs in: (info dict, definition file text or None).

    From inside the container: the image path apptainer/singularity reports
    (or the configured one), its size, modification time and head/tail fingerprint,
    the image labels (build date, base image, ...) and the definition file.
    """
    env = os.environ
    path = env.get("APPTAINER_CONTAINER") or env.get("SINGULARITY_CONTAINER") or image_hint or ""
    runtime = ("apptainer" if env.get("APPTAINER_CONTAINER") else
               "singularity" if env.get("SINGULARITY_CONTAINER") else
               "shifter" if env.get("SHIFTER_IMAGEREQUEST") or env.get("SHIFTER_RUNTIME") else "none")
    info = {"runtime": runtime, "path": path, "configured": image_hint or ""}
    if env.get("SHIFTER_IMAGEREQUEST"):
        info["shifter_image"] = env["SHIFTER_IMAGEREQUEST"]
    if path and os.path.isfile(path):
        st = os.stat(path)
        info.update({"size": st.st_size, "mtime": st.st_mtime,
                     "mtime_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(st.st_mtime)),
                     "head_tail_sha256": _head_tail_sha256(path)})
    elif path and os.path.isdir(path):
        info["sandbox"] = True
    labels, deffile = {}, None
    try:
        with open(os.path.join(SINGULARITY_D, "labels.json")) as f:
            labels = json.load(f)
    except (OSError, ValueError):
        pass
    try:
        with open(os.path.join(SINGULARITY_D, "Singularity"), errors="replace") as f:
            deffile = f.read()
    except OSError:
        pass
    info["labels"] = {str(k): str(v) for k, v in labels.items()}
    if labels.get("org.label-schema.build-date"):
        info["build_date"] = str(labels["org.label-schema.build-date"])
    if deffile:
        info["definition_sha256"] = sha256_text(deffile)
    return info, deffile


def parse_config(path, text):
    """Parse YAML/JSON config text into a dict (None if not parseable/applicable)."""
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in (".yaml", ".yml"):
            import yaml
            d = yaml.safe_load(text)
        elif ext == ".json":
            d = json.loads(text)
        else:
            return None
    except Exception:
        return None
    return d if isinstance(d, dict) else None


def _read_text(path):
    with open(path, errors="replace") as f:
        return f.read()


def collect(m, task, files=None, software=None, command=None, inputs=None, env=None):
    """Assemble this task's provenance record (plain dict).

    files    : {name: path} config files to embed (text + parsed tree)
    software : {name: path} repositories / executables to record
    """
    sc = m["stage_config"]
    cpath = os.path.join(m["campaign_dir"], "campaign.yaml")
    ctext = _read_text(cpath)
    rec = {
        "campaign": {"text": ctext, "path": cpath, "sha256": sha256_text(ctext),
                     "config": parse_config(cpath, ctext) or {}},
        "stage": {"stage": m["stage"], "alias": sc.get("alias") or "",
                  "handler": sc.get("handler") or "",
                  "stage_config": m.get("stage_config_raw") or {}},
        "block": "j%06d-%06d" % (task["first_job"], task["last_job"]),
        "attrs": {"task_id": task["task_id"], "attempt": task["attempt"],
                  "first_job": task["first_job"], "last_job": task["last_job"],
                  "host": os.uname().nodename, "created": time.time(),
                  "dprod_version": __version__, "image": m.get("image") or "",
                  "command": command or "", "slurm_job_id": os.environ.get("SLURM_JOB_ID", ""),
                  "campaign_config_sha256": sha256_text(ctext)},
        "seeds": dict(task["seeds"]),
        "files": {}, "config": {}, "software": {},
        "environment": dict(env or {}),
        "inputs": [os.path.basename(p) for p in (inputs or [])],
    }
    for name, path in (files or {}).items():
        text = _read_text(path)
        rec["files"][name] = {"text": text, "path": path, "sha256": sha256_text(text)}
        d = parse_config(path, text)
        if d is not None:
            rec["config"][name] = d
    for name, path in (software or {}).items():
        rec["software"][name] = software_info(path)
    info, deffile = container_info(m.get("image"))
    rec["container"] = info
    if deffile:
        rec["files"]["container_definition"] = {"text": deffile, "path": SINGULARITY_D + "/Singularity",
                                                "sha256": sha256_text(deffile)}
    return rec


# --------------------------------------------------------------------------- writing

def _link_text(prov, grp, name, text, sha):
    """grp[name] = text, stored once per file under /provenance/_blobs/<sha>."""
    blobs = prov.require_group(BLOBS)
    if sha not in blobs:
        blobs.create_dataset(sha, data=text, dtype=STR)
    grp[name] = blobs[sha]      # hard link


def _write_campaign(prov, rec):
    g = prov.create_group("campaign_config")
    _link_text(prov, g, "campaign.yaml", rec["text"], rec["sha256"])
    g.attrs["path"] = rec["path"]
    g.attrs["sha256"] = rec["sha256"]
    dict_to_group(g.create_group("config"), rec["config"])


def _write_task_block(prov, blk, rec):
    for k, v in rec["attrs"].items():
        blk.attrs[k] = v
    dict_to_group(blk.create_group("seeds"), rec["seeds"])
    fg = blk.create_group("files")
    for name, info in rec["files"].items():
        _link_text(prov, fg, name, info["text"], info["sha256"])
        fg.attrs[name + ".path"] = info["path"]
        fg.attrs[name + ".sha256"] = info["sha256"]
    dict_to_group(blk.create_group("config"), rec["config"])
    dict_to_group(blk.create_group("software"), rec["software"])
    dict_to_group(blk.create_group("container"), rec.get("container") or {})
    dict_to_group(blk.create_group("environment"), rec["environment"])
    blk.create_dataset("inputs", data=np.array(rec["inputs"], dtype=object), dtype=STR)


def _copy(src_prov, src, dst_prov, dst_parent, name):
    """Copy group `src` to dst_parent[name], re-linking file texts to dst blobs."""
    g = dst_parent.create_group(name)
    for k, v in src.attrs.items():
        g.attrs[k] = v
    for k, obj in src.items():
        if isinstance(obj, h5py.Group):
            _copy(src_prov, obj, dst_prov, g, k)
        elif src.name.endswith("/files") or src.name.endswith("/campaign_config"):
            text = _py(obj[()])
            _link_text(dst_prov, g, k, text, sha256_text(text))
        else:
            g.create_dataset(k, data=obj[()], dtype=obj.dtype)


def _merge_upstream(src_prov, prov, stages):
    """Merge the /provenance of an input file into prov (campaign + stage blocks)."""
    if "campaign_config" not in prov:
        _copy(src_prov, src_prov["campaign_config"], prov, prov, "campaign_config")
    # campaign config versions (after `dprod update-config`): keep every version an
    # input was produced with; each task block names its version (@campaign_config_sha256)
    versions = [src_prov["campaign_config"]]
    if "campaign_config_history" in src_prov:
        versions += list(src_prov["campaign_config_history"].values())
    for v in versions:
        sha = v.attrs["sha256"]
        if sha == prov["campaign_config"].attrs["sha256"]:
            continue
        hist = prov.require_group("campaign_config_history")
        if sha[:16] not in hist:
            _copy(src_prov, v, prov, hist, sha[:16])
    for stage in _py(src_prov.attrs["stages"]):
        s = src_prov[stage]
        if stage not in prov:
            d = prov.create_group(stage)
            for k, v in s.attrs.items():
                d.attrs[k] = v
            _copy(src_prov, s["stage_config"], prov, d, "stage_config")
        d = prov[stage]
        for blk in s:
            if blk != "stage_config" and blk not in d:
                _copy(src_prov, s[blk], prov, d, blk)
        if stage not in stages:
            stages.append(stage)


def write(path, rec, upstream=(), log=print):
    """Write /provenance into `path`: blocks copied from the `upstream` files
    (inputs the output was derived from), then this stage's block."""
    with h5py.File(path, "a") as f:
        if GROUP in f:
            del f[GROUP]
        prov = f.create_group(GROUP)
        prov.attrs["format_version"] = FORMAT_VERSION
        prov.attrs["campaign"] = rec["campaign"]["config"].get("campaign", "")
        stages = []
        for up in upstream:
            with h5py.File(up, "r") as s:
                if GROUP not in s or "stages" not in s[GROUP].attrs:
                    log("warning: input %s has no (current) provenance" % os.path.basename(up))
                    continue
                _merge_upstream(s[GROUP], prov, stages)
        if "campaign_config" in prov and \
                prov["campaign_config"].attrs["sha256"] != rec["campaign"]["sha256"]:
            # upstream used another version: move it to the history, current on top
            hist = prov.require_group("campaign_config_history")
            old = prov["campaign_config"].attrs["sha256"][:16]
            if old not in hist:
                prov.move("campaign_config", "campaign_config_history/" + old)
            else:
                del prov["campaign_config"]
        if "campaign_config" not in prov:
            _write_campaign(prov, rec["campaign"])
        st = rec["stage"]
        if st["stage"] not in prov:
            sg = prov.create_group(st["stage"])
            for k in ("stage", "alias", "handler"):
                sg.attrs[k] = st[k]
            dict_to_group(sg.create_group("stage_config"), st["stage_config"])
        sg = prov[st["stage"]]
        if rec["block"] in sg:
            del sg[rec["block"]]
        _write_task_block(prov, sg.create_group(rec["block"]), rec)
        if st["stage"] not in stages:
            stages.append(st["stage"])
        prov.attrs.create("stages", np.array(stages, dtype=object), dtype=STR)


# --------------------------------------------------------------------------- reading

def read_provenance(path):
    """/provenance of a file as a nested dict (None if absent)."""
    with h5py.File(path, "r") as f:
        if GROUP not in f:
            return None
        return group_to_dict(f[GROUP])


def task_blocks(prov, stage):
    """Task blocks of a stage in a read_provenance() dict, sorted by job range."""
    s = prov.get(stage) or {}
    return [s[k] for k in sorted(s) if k.startswith("j") and isinstance(s[k], dict)]


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        print("usage: python3 -m doraemon_prod.provenance <file.h5> [--files]")
        return 2
    p = read_provenance(argv[0])
    if p is None:
        print("%s: no /%s group" % (argv[0], GROUP))
        return 1
    if "--files" not in argv:   # file texts can be long; show sizes only
        def hide(d):
            for k, v in d.items():
                if isinstance(v, dict):
                    hide(v)
                elif isinstance(v, str) and (k == "campaign.yaml" or "\n" in v) and len(v) > 80:
                    d[k] = "<text, %d chars>" % len(v)
        hide(p)
    print(json.dumps(p, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
