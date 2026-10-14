"""Storage budget (controller side): submit only what the storage area can hold.

Configured in the campaign config (all optional; without them nothing changes):

    storage:
      limit: 40T          # what this campaign's files may occupy in total
      min_free: 2T        # keep this much free on the storage filesystem (as `df` shows it)
      probe_jobs: 3       # per stage: tasks allowed out before a finished one gives an estimate
      cancel_queued: true # advance/watch cancel pending elements that would not fit

Jobs copy their outputs to storage when they finish, so every queued or running
element reserves its stage's per-task estimate: the mean size of a finished task's
outputs plus its attempt's logs and summaries, plus 10%. Before a stage has a
finished task it has no estimate, and at most `probe_jobs` of its tasks may be
queued or running (while there is room at all).

headroom = min(limit - used, free - min_free) - reserved by queued/running elements.
New submissions go to downstream stages first (they turn upstream outputs into
results), then upstream; when the headroom is negative, `advance` cancels pending
elements, upstream stages and highest task ids first. A cancellation does not use
an attempt; the tasks are submitted again when there is room.
"""

import os
import re

from . import layout as L

MARGIN = 1.1            # estimates are padded by this factor
AUX_SAMPLE = 20         # finished attempts per stage whose logs/summaries are measured

_UNITS = {"": 1, "B": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4, "P": 1024 ** 5}

DEFAULTS = {"limit": None, "min_free": None, "probe_jobs": 3, "cancel_queued": True}


def parse_size(v):
    """'40T', '500G', '1.5TB', 2e12 -> bytes (binary units); None -> None."""
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError("not a size: %r" % v)
    if isinstance(v, (int, float)):
        return int(v)
    m = re.match(r"^\s*([0-9.]+)\s*([KMGTP]?)(I?B)?\s*$", str(v).upper())
    if not m:
        raise ValueError("not a size: %r (e.g. 40T, 500G)" % v)
    return int(float(m.group(1)) * _UNITS[m.group(2)])


def fmt(n):
    if n is None:
        return "-"
    sign = "-" if n < 0 else ""
    n = abs(n)
    for unit in ("B", "K", "M", "G", "T", "P"):
        if n < 1024 or unit == "P":
            return sign + (("%.0f%s" if unit == "B" else "%.1f%s") % (n, unit))
        n /= 1024.0


def depth(cfg, stage):
    d = 0
    while cfg["stages"][stage]["parent"]:
        stage = cfg["stages"][stage]["parent"]
        d += 1
    return d


class Budget:
    """Snapshot of the campaign's storage use, taken after a sync."""

    def __init__(self, c):
        self.c = c
        sc = dict(DEFAULTS, **(c.cfg.get("storage") or {}))
        self.limit = parse_size(sc["limit"])
        self.min_free = parse_size(sc["min_free"])
        self.probe_jobs = int(sc["probe_jobs"] or 0)
        self.cancel_queued = bool(sc["cancel_queued"])
        self.enabled = self.limit is not None or self.min_free is not None
        if not self.enabled:
            return
        con = c.con
        self.stages = [st for st, s in c.cfg["stages"].items() if not s.get("external")]
        self.estimate, self.n_done, self.active = {}, {}, {}
        self.used = 0
        reserved = 0
        for st in self.stages:
            out_mean, n = con.execute(
                "SELECT AVG(sz), COUNT(*) FROM (SELECT SUM(COALESCE(f.size, 0)) sz FROM files f"
                " JOIN tasks t ON t.stage = f.stage AND t.task_id = f.task_id"
                " WHERE f.stage = ? AND t.status = 'done' GROUP BY f.task_id)", (st,)).fetchone()
            aux = self._aux_mean(st)
            self.n_done[st] = n
            self.estimate[st] = MARGIN * ((out_mean or 0) + aux) if n else None
            outputs = con.execute("SELECT COALESCE(SUM(size), 0) FROM files WHERE stage = ?",
                                  (st,)).fetchone()[0]
            finished = con.execute("SELECT COUNT(*) FROM attempts WHERE stage = ? AND state IN"
                                   " ('done', 'failed')", (st,)).fetchone()[0]
            self.used += outputs + aux * finished
            self.active[st] = c.n_active(st)
            reserved += self.active[st] * (self.estimate[st] or 0)
        self.reserved = reserved
        self.free = None
        if self.min_free is not None:
            try:
                v = os.statvfs(c.dir)
                self.free = v.f_bavail * v.f_frsize
            except OSError:
                pass
        room = []
        if self.limit is not None:
            room.append(self.limit - self.used)
        if self.min_free is not None and self.free is not None:
            room.append(self.free - self.min_free)
        # (min_free alone and df failed: no constraint known)
        self.headroom = (min(room) if room else float("inf")) - self.reserved

    def _aux_mean(self, stage):
        """Mean size of what an attempt leaves besides its outputs (log tarball,
        summaries, job record), measured on recently finished attempts."""
        rows = self.c.con.execute(
            "SELECT t.first_job, t.last_job, a.attempt FROM attempts a JOIN tasks t"
            " ON t.stage = a.stage AND t.task_id = a.task_id WHERE a.stage = ? AND a.state = 'done'"
            " ORDER BY a.end_time DESC LIMIT ?", (stage, AUX_SAMPLE)).fetchall()
        sizes = []
        for first, last, att in rows:
            name = L.task_name(stage, first, last)
            n = 0
            for rel in (L.logs_rel_path(stage, name, att), L.summary_rel_path(stage, name, att),
                        L.job_summary_rel_path(stage, name, att),
                        os.path.join("records", stage, "%s_a%02d.json" % (name, att))):
                try:
                    n += os.path.getsize(os.path.join(self.c.dir, rel))
                except OSError:
                    pass
            sizes.append(n)
        return sum(sizes) / len(sizes) if sizes else 0

    # ------------------------------------------------------------------ submissions
    def trim(self, items, out=print):
        """Cut a plan [(stage, recovery, entries)] to what fits; downstream stages first.
        Returns the trimmed plan (same order, empty items dropped)."""
        if not self.enabled:
            return items
        cfg = self.c.cfg
        room = self.headroom
        keep = {}
        order = sorted(range(len(items)), key=lambda i: -depth(cfg, items[i][0]))
        probing = {}
        for i in order:
            stage, recovery, entries = items[i]
            est = self.estimate.get(stage)
            if est is None:          # no finished task yet: a few probe jobs, if there is room
                used = self.active.get(stage, 0) + probing.get(stage, 0)
                n = max(0, self.probe_jobs - used) if room > 0 else 0
                probing[stage] = probing.get(stage, 0) + min(n, len(entries))
            else:
                n = len(entries) if est <= 0 or room == float("inf") else max(0, int(room // est))
            n = min(n, len(entries))
            if n < len(entries):
                why = ("probing: %d task(s) until one finishes" % self.probe_jobs if est is None
                       else "headroom %s, ~%s per task" % (fmt(max(room, 0)), fmt(est)))
                out("storage: %s %s limited to %d of %d task(s) (%s)" % (
                    stage, "retries" if recovery else "new tasks", n, len(entries), why))
            keep[i] = entries[:n]
            if est is not None:
                room -= n * est
        return [(st, rec, keep[i]) for i, (st, rec, _) in enumerate(items) if keep[i]]

    def to_cancel(self):
        """Pending elements to cancel so that the reservations fit again:
        [attempt rows], upstream stages and highest task ids first."""
        if not self.enabled or self.headroom >= 0:
            return []
        need = -self.headroom
        out = []
        for st in sorted(self.stages, key=lambda s: depth(self.c.cfg, s)):
            est = self.estimate.get(st)
            if not est:
                continue                 # reserves nothing; cancelling it frees nothing
            rows = self.c._selected_active(st, None, queued_only=True)
            for r in sorted(rows, key=lambda r: -r["task_id"]):
                if need <= 0:
                    return out
                out.append(r)
                need -= est
        return out

    # ------------------------------------------------------------------ report
    def lines(self):
        if not self.enabled:
            return []
        parts = ["used %s" % fmt(self.used)]
        if self.limit is not None:
            parts[0] += " of limit %s" % fmt(self.limit)
        if self.min_free is not None:
            parts.append("free %s (keep %s)" % (fmt(self.free), fmt(self.min_free)))
        parts.append("reserved %s for %d queued/running" % (fmt(self.reserved), sum(self.active.values())))
        state = "headroom %s" % fmt(self.headroom)
        if self.headroom <= 0:
            state += ": no new submissions"
        cfg = self.c.cfg
        est = ", ".join("%s %s" % (cfg["stages"][st]["alias"] or st,
                                    fmt(self.estimate[st]) if self.estimate[st] is not None
                                    else "probing (%d)" % self.probe_jobs)
                        for st in self.stages)
        return ["storage: %s; %s" % ("; ".join(parts), state), "  per-task estimate: %s" % est]
