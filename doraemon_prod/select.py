"""Choosing the site, campaign config and campaign (controller side).

Order of precedence for site / campaign:
  1. --site / --campaign on the command line
  2. DPROD_SITE / DPROD_CAMPAIGN environment variables
  3. the last one used, remembered in ~/.config/dprod/state.json (DPROD_STATE overrides the path)
  4. an interactive numbered menu (only on a terminal)
"""

import glob
import json
import os
import sqlite3
import sys
import time

from . import config as C


class SelectError(Exception):
    pass


# --------------------------------------------------------------------------- remembered state

def _state_path():
    p = os.environ.get("DPROD_STATE")
    if p is not None:
        return p or None                  # DPROD_STATE="" disables remembering
    return os.path.join(os.path.expanduser("~"), ".config", "dprod", "state.json")


def load_state():
    p = _state_path()
    try:
        with open(p) as f:
            return json.load(f)
    except (TypeError, OSError, ValueError):
        return {}


def save_state(site, campaign=None):
    p = _state_path()
    if not p:
        return
    st = load_state()
    st["site"] = site
    if campaign:
        st["campaign"] = campaign
        st.setdefault("recent", [])
        st["recent"] = [campaign] + [x for x in st["recent"] if x != campaign][:9]
    st["saved"] = time.time()
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp.%d" % os.getpid()
        with open(tmp, "w") as f:
            json.dump(st, f, indent=1)
        os.replace(tmp, p)
    except OSError:
        pass


# --------------------------------------------------------------------------- discovery

def list_sites():
    """[(name, path, site dict or None, usable_here)] for configs/sites/*.yaml."""
    out = []
    for p in sorted(glob.glob(os.path.join(C.SITE_DIR, "*.yaml"))):
        name = os.path.splitext(os.path.basename(p))[0]
        try:
            s = C.load_site(p, check_paths=False)
        except Exception:
            out.append((name, p, None, False))
            continue
        root = s["storage_root"]
        usable = "$" not in root and (os.path.isdir(root) or os.path.isdir(os.path.dirname(root)))
        out.append((name, p, s, usable))
    return out


def list_configs():
    """[(path, tag, description)] for configs/campaigns/*.yaml."""
    out = []
    for p in sorted(glob.glob(os.path.join(C.REPO_DIR, "configs", "campaigns", "*.yaml"))):
        try:
            d = C.load_yaml(p)
            out.append((p, d.get("campaign", "?"), " ".join(str(d.get("description") or "").split())))
        except Exception as e:
            out.append((p, "?", "unreadable: %s" % e))
    return out


def list_campaigns(site):
    """Campaigns under a site's storage root, most recently active first:
    [{tag, dir, active (mtime), stages: [(alias, done, total)]}]."""
    root = site["storage_root"]
    out = []
    for cfg in glob.glob(os.path.join(root, "*", "campaign.yaml")):
        d = os.path.dirname(cfg)
        db = os.path.join(d, "bookkeeping.sqlite")
        info = {"tag": os.path.basename(d), "dir": d, "stages": [],
                "active": os.path.getmtime(db) if os.path.exists(db) else os.path.getmtime(cfg)}
        try:
            con = sqlite3.connect("file:%s?mode=ro" % db, uri=True, timeout=5)
            rows = con.execute("SELECT stage, SUM(status = 'done'), COUNT(*) FROM tasks GROUP BY stage").fetchall()
            aliases = {}
            try:
                camp = C.load_campaign(cfg)
                aliases = {n: s["alias"] or n for n, s in camp["stages"].items()}
                order = list(camp["stages"])
            except Exception:
                order = [r[0] for r in rows]
            by = {r[0]: r for r in rows}
            info["stages"] = [(aliases.get(n, n), by[n][1] or 0, by[n][2]) for n in order if n in by]
            con.close()
        except sqlite3.Error:
            pass
        out.append(info)
    out.sort(key=lambda x: -x["active"])
    return out


# --------------------------------------------------------------------------- menus

def _interactive():
    return sys.stdin.isatty() and os.environ.get("DPROD_BATCH", "") in ("", "0")


def choose(title, options, default=None, what="option"):
    """Numbered menu. options: [(value, label)]. Returns the chosen value."""
    if not options:
        raise SelectError("no %s to choose from" % what)
    if not _interactive():
        raise SelectError("%s needed: %s" % (what, ", ".join(str(v) for v, _ in options)))
    print(title)
    dflt = None
    for i, (v, label) in enumerate(options, 1):
        mark = ""
        if v == default:
            dflt, mark = i, "  (default)"
        print("  %2d) %s%s" % (i, label, mark))
    while True:
        ans = input("choose [1-%d]%s: " % (len(options), " (Enter = %d)" % dflt if dflt else "")).strip()
        if not ans and dflt:
            return options[dflt - 1][0]
        if ans.isdigit() and 1 <= int(ans) <= len(options):
            return options[int(ans) - 1][0]
        for v, _ in options:
            if ans == str(v):
                return v
        print("  not a valid choice")


def pick_site(explicit=None, note=None):
    """Site name: explicit > env (argparse default) > remembered > menu."""
    if explicit:
        return explicit
    st = load_state()
    if st.get("site"):
        if note:
            note("site %s (remembered; --site to override)" % st["site"])
        return st["site"]
    sites = list_sites()
    usable = [n for n, _, s, u in sites if u]
    if not _interactive() and len(usable) == 1:
        return usable[0]
    opts = [(n, "%-8s %s%s" % (n, s["storage_root"] if s else "(invalid config)",
                                "   <- usable on this machine" if u else ""))
            for n, _, s, u in sites]
    return choose("Site:", opts, default=usable[0] if len(usable) == 1 else None, what="--site")


def pick_campaign(site, explicit=None, note=None):
    """Campaign tag: explicit > env > remembered (same site) > menu of existing campaigns."""
    if explicit:
        return explicit
    st = load_state()
    if st.get("campaign") and st.get("site") == site["name"] and \
            os.path.exists(os.path.join(site["storage_root"], st["campaign"], "campaign.yaml")):
        if note:
            note("campaign %s at site %s (remembered; `dprod use` to switch)" % (st["campaign"], site["name"]))
        return st["campaign"]
    camps = list_campaigns(site)
    if not camps:
        raise SelectError("no campaigns at site %s (%s); create one with `dprod init`" % (
            site["name"], site["storage_root"]))
    opts = [(c["tag"], "%-34s %s" % (c["tag"], progress_text(c))) for c in camps]
    return choose("Campaign at site %s:" % site["name"], opts, default=camps[0]["tag"], what="--campaign")


def progress_text(c):
    age = (time.time() - c["active"]) / 3600.0
    when = "%.0f min ago" % (age * 60) if age < 2 else "%.1f h ago" % age if age < 48 else "%.0f days ago" % (age / 24)
    stages = "  ".join("%s %d/%d" % s for s in c["stages"])
    return "active %-13s %s" % (when, stages)
