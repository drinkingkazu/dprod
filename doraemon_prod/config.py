"""Site and campaign configuration (controller side, needs PyYAML).

Site config (configs/sites/<name>.yaml): everything that depends on the
computing center -- storage/log/work directories, container runtime, image
paths, slurm account/partition, software locations.  Loaded live from the repo
so operational changes (e.g. a new account) apply to future submissions.

Campaign config (configs/campaigns/<tag>.yaml): the physics/production
definition -- stages, job counts, merge factors, commands.  Frozen into the
campaign directory at `init` and read from there afterwards.
"""

import copy
import os

import yaml

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE_DIR = os.path.join(REPO_DIR, "configs", "sites")

HANDLERS = ("edepsim", "command")

SITE_DEFAULTS = {
    "scheduler": "slurm",          # slurm | local
    "max_array_size": 100,
    "monitor_interval": 10,        # seconds between resource samples in jobs
    "db_path": None,               # default: <campaign_dir>/bookkeeping.sqlite
    "work_root": "/tmp",           # expanded on the compute node
    "container": {"exec": "{image}", "flags": ""},
    "images": {},
    "vars": {},
    "env": {},
    "slurm": {"default": {}, "profiles": {}},
}

STAGE_DEFAULTS = {
    "alias": None,
    "parent": None,
    "merge": 1,
    "handler": "command",
    "image": None,
    "profile": None,
    "slurm": {},
    "max_concurrent": None,
    "container_flags": None,
    "env": {},
    # stage-1 (edepsim handler)
    "n_jobs": 0,
    "events_per_job": None,
    "input_dir": None,
    "geometry": None,
    "macro": None,
    "generator_config": None,
    # downstream (command handler)
    "command": None,
    "outputs": "**/*.h5",
    "id_reader": None,
    "id_reader_options": {},
    "strict_events": False,
    "input_roles": None,           # restrict parent file roles passed as {inputs}
    "also_inputs": [],             # ancestor stages whose files are passed as {inputs_<stage>}
    "enabled": True,               # false: defined and tracked, but submit refuses
    "vars": {},                    # stage-level template variables (may use site vars)
    # provenance embedded into outputs (/provenance group); paths may use {vars}
    #   roles: output roles that get it (None = all); files: {name: config file};
    #   software: {name: repository dir or executable}
    "provenance": {"roles": None, "files": {}, "software": {}},
    "external": None,              # set for stages inherited from another campaign (read-only)
    "external_dir": None,          # that campaign's directory
    "max_queued": None,
    "check_imports": ["h5py"],     # python modules `dprod check` imports inside the stage image
    "pythonpath": [],              # dirs put first on PYTHONPATH for the stage command (and for
                                   # `dprod check`), e.g. ["{pysupera_dir}"]; may use {vars}            # advance/watch: keep at most this many elements queued+running
    "monitor_gpu": None,           # sample GPU use; None = auto (slurm options request GPUs)
    "monitor_interval": None,      # seconds between resource samples (default: site, 10)
}


class ConfigError(Exception):
    pass


def _deep_update(base, extra):
    out = copy.deepcopy(base)
    for k, v in (extra or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_update(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _expand(path):
    return os.path.expanduser(os.path.expandvars(path)) if path else path


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_site(name_or_path, check_paths=True):
    path = name_or_path
    if not os.path.exists(path):
        path = os.path.join(SITE_DIR, name_or_path + ".yaml")
    if not os.path.exists(path):
        raise ConfigError("site config not found: %s" % name_or_path)
    site = _deep_update(SITE_DEFAULTS, load_yaml(path))
    for key in ("name", "storage_root", "log_root"):
        if not site.get(key):
            raise ConfigError("site config %s: missing '%s'" % (path, key))
    if site["scheduler"] not in ("slurm", "local"):
        raise ConfigError("site %s: unknown scheduler %r" % (site["name"], site["scheduler"]))
    # Controller-side paths are expanded now; work_root is expanded on the node.
    site["storage_root"] = _expand(site["storage_root"])
    site["log_root"] = _expand(site["log_root"])
    site["db_path"] = _expand(site["db_path"])
    for key in ("storage_root", "log_root", "db_path"):
        if check_paths and site[key] and "$" in site[key]:
            raise ConfigError("site %s: %s=%r has an undefined environment variable" % (
                site["name"], key, site[key]))
    site["_path"] = os.path.abspath(path)
    return site


def load_campaign(path, site=None):
    cfg = load_yaml(path)
    if needs_materialize(cfg):
        if site is None:
            raise ConfigError("%s inherits stages from campaign %r; a site is needed to resolve them"
                              % (path, (cfg.get("inherit") or {}).get("campaign")))
        cfg = materialize_inherit(cfg, site, source=path)
    return normalize_campaign(cfg, source=path)


def needs_materialize(raw):
    inh = raw.get("inherit") or {}
    if not inh:
        return False
    stages = raw.get("stages") or {}
    return not all((stages.get(n) or {}).get("external") for n in inh.get("stages") or [])


def materialize_inherit(raw, site, source="<campaign>"):
    """Resolve `inherit: {campaign: <tag>, stages: [...]}`: copy those stage definitions
    (and their ancestors) from the source campaign's frozen config, marked external."""
    raw = copy.deepcopy(raw)
    inh = raw.get("inherit") or {}
    tag = inh.get("campaign")
    if not tag or not inh.get("stages"):
        raise ConfigError("%s: inherit needs 'campaign' and 'stages'" % source)
    src_dir = os.path.join(site["storage_root"], tag)
    src_cfg = os.path.join(src_dir, "campaign.yaml")
    if not os.path.exists(src_cfg):
        raise ConfigError("%s: inherited campaign %s not found at %s" % (source, tag, src_dir))
    src = load_yaml(src_cfg)
    src_stages = src.get("stages") or {}
    want, todo = [], list(inh["stages"])
    while todo:
        n = todo.pop()
        if n not in src_stages:
            raise ConfigError("%s: campaign %s has no stage %r (it has: %s)" % (
                source, tag, n, ", ".join(src_stages)))
        if n not in want:
            want.append(n)
            if src_stages[n].get("parent"):
                todo.append(src_stages[n]["parent"])
    own = raw.get("stages") or {}
    clash = [n for n in want if n in own and not (own[n] or {}).get("external")]
    if clash:
        raise ConfigError("%s: stage(s) %s are inherited from %s and cannot be redefined; "
                          "give the new stage another name" % (source, ", ".join(clash), tag))
    merged = {}
    for n in src_stages:                       # keep the source's stage order
        if n in want:
            st = copy.deepcopy(src_stages[n])
            st["external"] = st.get("external") or tag       # a chain keeps the original owner
            st["external_dir"] = st.get("external_dir") or src_dir
            merged[n] = st
    for n, st in own.items():
        if n not in merged:
            merged[n] = st
    raw["stages"] = merged
    return raw


def normalize_campaign(cfg, source="<campaign>"):
    cfg = copy.deepcopy(cfg)
    tag = cfg.get("campaign")
    if not tag or "/" in tag or " " in tag:
        raise ConfigError("%s: 'campaign' must be a non-empty tag without '/' or spaces" % source)
    cfg.setdefault("description", "")
    cfg.setdefault("seed", 0)
    cfg.setdefault("max_attempts", 3)
    raw_stages = cfg.get("stages") or {}
    if not raw_stages:
        raise ConfigError("%s: no stages defined" % source)

    stages = {}
    for name, sc in raw_stages.items():
        s = _deep_update(STAGE_DEFAULTS, sc)
        s["name"] = name
        if s["handler"] not in HANDLERS:
            raise ConfigError("stage %s: handler must be one of %s" % (name, HANDLERS))
        if s["alias"] is not None:
            s["alias"] = str(s["alias"])
        stages[name] = s

    roots = [s for s in stages.values() if not s["parent"]]
    if len(roots) != 1:
        raise ConfigError("%s: exactly one root stage (no parent) is required" % source)
    for s in stages.values():
        if s["parent"] and s["parent"] not in stages:
            raise ConfigError("stage %s: unknown parent %s" % (s["name"], s["parent"]))
        if s["handler"] == "edepsim":
            if s["parent"]:
                raise ConfigError("stage %s: edepsim handler must be the root stage" % s["name"])
            for key in ("events_per_job", "input_dir", "geometry", "macro"):
                if not s[key]:
                    raise ConfigError("stage %s: missing '%s'" % (s["name"], key))
        else:
            if not s["parent"]:
                raise ConfigError("stage %s: root stage must use the edepsim handler" % s["name"])
            if not s["command"] or not s["id_reader"]:
                raise ConfigError("stage %s: 'command' and 'id_reader' are required" % s["name"])
        for anc in s["also_inputs"]:
            if anc not in stages:
                raise ConfigError("stage %s: also_inputs has unknown stage %s" % (s["name"], anc))
        if int(s["merge"]) < 1:
            raise ConfigError("stage %s: merge must be >= 1" % s["name"])
        s["merge"] = int(s["merge"])

    # topological order: root first, children after parents
    order, pending = [], dict(stages)
    while pending:
        progressed = False
        for name in list(pending):
            p = pending[name]["parent"]
            if p is None or p in order:
                order.append(name)
                del pending[name]
                progressed = True
        if not progressed:
            raise ConfigError("%s: cyclic stage parents" % source)
    cfg["stages"] = {n: stages[n] for n in order}
    cfg["raw_stages"] = {n: copy.deepcopy(raw_stages[n] or {}) for n in order}  # as written
    cfg["root_stage"] = order[0]
    return cfg


def resolve_stage(campaign, name):
    """Accept a stage name or alias (e.g. '1', '2A')."""
    if name in campaign["stages"]:
        return name
    for s in campaign["stages"].values():
        if s["alias"] is not None and s["alias"].lower() == str(name).lower():
            return s["name"]
    raise ConfigError("unknown stage %r (known: %s)" % (
        name, ", ".join("%s[%s]" % (s["name"], s["alias"]) for s in campaign["stages"].values())))


def slurm_options(site, stage):
    """Merge slurm options: site default < site profile < campaign stage override."""
    opts = dict(site["slurm"].get("default") or {})
    prof = stage["profile"]
    if prof:
        profiles = site["slurm"].get("profiles") or {}
        if prof not in profiles:
            raise ConfigError("site %s has no slurm profile %r (stage %s)" % (
                site["name"], prof, stage["name"]))
        opts.update(profiles[prof] or {})
    opts.update(stage["slurm"] or {})
    opts.pop("container_flags", None)   # not a slurm option; see container_prefix
    return opts


def container_prefix(site, stage, login=False):
    """Command prefix that runs something inside the stage's container image.

    login=True: for commands the controller runs on the login/interactive node
    (e.g. merge-summary); uses container.exec_login if set (binds that only
    exist on compute nodes, like /lscratch, would fail there).
    Returns "" when the site runs without containers (e.g. local testing).
    """
    tmpl = site["container"].get("exec") or ""
    if login and site["container"].get("exec_login"):
        tmpl = site["container"]["exec_login"]
    if not tmpl.strip() or tmpl.strip() == "{image}":
        return ""
    image = None
    if stage["image"]:
        image = site["images"].get(stage["image"])
        if not image:
            raise ConfigError("site %s: no image %r (stage %s)" % (
                site["name"], stage["image"], stage["name"]))
    flags = stage["container_flags"]
    if flags is None:
        prof = (site["slurm"].get("profiles") or {}).get(stage["profile"] or "", {}) or {}
        flags = site["container"].get("flags", "")
        # a slurm profile may add container flags (e.g. --nv for GPU nodes)
        flags = " ".join(x for x in (flags, prof.get("container_flags", "")) if x)
    return tmpl.format(image=image or "", flags=flags).strip()


def stage_env(site, stage):
    env = dict((site["env"] or {}).get("all") or {})
    env.update((site["env"] or {}).get(stage["name"]) or {})
    env.update(stage["env"] or {})
    return {k: str(v) for k, v in env.items()}


GPU_SLURM_KEYS = ("gpus", "gpus_per_node", "gpus_per_task", "gpus_per_socket", "gres")


def monitor_settings(site, stage):
    gpu = stage["monitor_gpu"]
    if gpu is None:
        opts = slurm_options(site, stage)
        gpu = any(opts.get(k) and (k != "gres" or "gpu" in str(opts[k])) for k in GPU_SLURM_KEYS)
    interval = stage["monitor_interval"] or site.get("monitor_interval") or 10
    return {"gpu": bool(gpu), "interval": float(interval)}


def slurm_time_s(t):
    """slurm --time value ("MM", "MM:SS", "HH:MM:SS", "D-HH[:MM[:SS]]") -> seconds."""
    if t is None or t == "":
        return None
    t = str(t)
    days = 0
    if "-" in t:
        d, t = t.split("-", 1)
        days = int(d)
        parts = [int(x) for x in t.split(":")] + [0, 0]
        h, m, sec = parts[0], parts[1], parts[2]
    else:
        parts = [int(x) for x in t.split(":")]
        if len(parts) == 1:
            h, m, sec = 0, parts[0], 0
        elif len(parts) == 2:
            h, m, sec = 0, parts[0], parts[1]
        else:
            h, m, sec = parts[:3]
    return ((days * 24 + h) * 60 + m) * 60 + sec
