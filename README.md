# DORAEMON production control (`dprod`)

Controls and keeps the books for the multi-stage DORAEMON production:

| stage | alias | what | handler |
|---|---|---|---|
| `edepsim` | 1 | generator + Geant4 (edep-sim, HDF5 output) | built-in `edepsim` |
| `jaxtpc_wire` / `jaxtpc_pixel` | 2A / 2B | JAXTPC detector simulation | `command` |
| `supera_wire` / `supera_pixel` | 3A / 3B | pysupera labels, via `doraemon_prod.stages.supera` | `command` |

## Concepts

* **Campaign**: a unique tag (e.g. `test_doraemon_2026_v0.1`) + a config in
  `configs/campaigns/`. Everything lives in `<storage_root>/<campaign>/`.
* **Job id**: 0-indexed and unique across all arrays of a campaign. There is one stage-1
  task per job id. edep-sim is told `/edep/runId <job id>`, so the Geant4
  `run_id` in the output *is* the job id. **Event id** is the Geant4 event
  number (0..N-1). `(job id, event id)` identifies an event in every stage.
* **Task**: one unit of work at a stage, which is one slurm array element. A task covers a
  consecutive job range. Downstream tasks group `merge` consecutive parent
  tasks. Names encode the range, e.g. `edepsim_j000123-000123.h5` and
  `jaxtpc_wire_j000120-000124/`.
* **Seeds**: derived deterministically from `(campaign seed, job id)`. The macro's
  `timeRandomSeed` and the ParticleBomb `SEED: -1` (which means clock-based) are replaced. A retry
  reproduces the same events, and `recover --reseed` gives new ones.
* **Bookkeeping**: `bookkeeping.sqlite` is written *only* by the controller. Each
  job attempt writes a JSON summary (status, timing, and every `(job, event)`
  in every output file). `dprod sync` merges these with `sacct` states.
* **Streaming**: a downstream task becomes submittable as soon as all of its
  parent tasks are `done` or `abandoned`. The whole upstream stage doesn't have to finish first.
  `dprod advance` submits everything that is ready in every stage, and `dprod watch` repeats it
  until the campaign is finished (see below).

Task states: `new → submitted → running → done | failed`. A failed task can be
resubmitted until `max_attempts`, or it can be marked `abandoned` so downstream proceeds without it.

## Usage

You don't need to set any environment variables. Commands act on the **current campaign**, which is the one you last
created (`init`), switched to (`use`) or picked from a menu. It's remembered in `~/.config/dprod/state.json`, and
each command prints a note when it uses it. If there's none, you get a numbered menu of the site's campaigns.
`--site`/`--campaign` (or `DPROD_SITE`/`DPROD_CAMPAIGN`) override it for one command, without changing it, so
scrontab rounds that name their campaigns never switch yours. The site defaults to the one whose storage path
exists on this machine (s3df on sdfiana).

The controller runs on a login node: python3 (≥ 3.6) + PyYAML, plus `sbatch`/`sacct`.

```bash
bin/dprod init                  # asks: site, campaign config (shows the tag it contains), confirm
bin/dprod sites                 # site configs; which one is usable on this machine
bin/dprod configs               # campaign configs and the campaign tag in each
bin/dprod campaigns             # existing campaigns at the site with progress; * = current
bin/dprod use [<tag>]           # switch the current campaign (menu without a tag)

dprod submit 1 --limit 300      # split into arrays of <=100 (S3DF limit)
dprod status                    # sync + table: queued/running/done/failed, events, wall time
dprod submit 2A                 # whatever stage-2A tasks have their inputs ready
dprod advance --recover         # every stage at once: all ready tasks, plus retries of failures
dprod watch --recover --web     # repeat `advance` every 10 min until nothing can progress,
                                # updating the monitoring page each round
dprod web                       # write the monitoring page once
dprod failures 1 -v             # reasons, slurm ids, log tarballs; for jobs that died before
                                # the worker started (e.g. container failure) the job log's error line
dprod recover 1                 # resubmit failed tasks (attempt < max_attempts) and cancelled ones
dprod reset-attempts 1          # start the max_attempts count afresh for failed tasks
dprod mark 1 17,20-25 --abandon --note "reason"
dprod lookup 123 45             # all files (every stage/role) holding job 123 event 45
dprod files 2A -l               # registered outputs with job ranges and event counts
dprod extend 2000               # more stage-1 jobs; downstream tasks are defined automatically
dprod cancel 2A --queued        # scancel queued elements; their tasks go back to 'new'
dprod move 2A --partition P --account A   # re-route queued elements in place (scontrol update)
dprod check <campaign.yaml>     # preflight: tools, paths, images, software (before init)
dprod update-config <new.yaml>  # change a running campaign's config (checked against what ran)
dprod destroy                   # cancel all jobs, delete all files of the campaign (asks for the tag)
```

### Confirmation before submitting

`submit`, `recover` and `advance` first print what they are about to submit, then ask `Proceed? [y/N]`:
```
Submission plan for campaign test_doraemon_2026_v0.1 (site s3df):
  stage kind   tasks arrays  stage-1 jobs    events  partition  account            qos          time      gpus
  1     new      300      3   300 (0-299)    60,000  milano     mli:nu-ml-dev      preemptable  00:20:00  -
  2A    new       12      1     60 (0-59)    12,000  ampere     neutrino:ml-dev    -            02:00:00  1
  total: 312 task(s) in 4 array(s), 72,000 events
```
Exactly the plan you confirm is submitted. `--batch` (or `-y`, or `DPROD_BATCH=1` in the environment)
skips the question, and the summary is still printed. Without a terminal (scripts), a command refuses to submit
unless batch mode is on. `watch` and `dprod-cron` never ask, but they log the plan every round. `--dry-run` shows
the plan and writes the scripts without submitting.

### Keeping all stages moving: `advance` and `watch`

`dprod advance` syncs with slurm, then goes through the stages in order (1, 2A, 2B, 3A, 3B).
For each one it submits every task whose inputs are ready, after the confirmation above. It plans once:
stage-2 tasks that become ready only after this call's stage-1 jobs finish go out on a later call. With `--recover` it also resubmits failed tasks that
still have attempts left. So stage 2A task *k* goes out as soon as the stage-1 jobs it needs are
done, while other stage-1 jobs are still queued.

`dprod watch` repeats that every `--interval` seconds (default 600) and prints the status table.
It stops by itself when nothing is queued, running or ready. If some tasks are still failed or blocked at that point,
`dprod failures` shows why. Run it in `tmux`/`screen` on sdfiana so it survives logging out:

```bash
tmux new -s doraemon
bin/dprod watch --recover --merge-summary --web   # Ctrl-b d to detach; tmux attach -t doraemon
```
`dprod watch --once` does a single round, for cron-style use.

* **Throttle:** `--max-queued N`, or `max_queued: N` in a stage of the campaign config, keeps at most N array
  elements of a stage queued or running. This lets a 5000-job campaign go in gradually.
* **Lock:** every command that changes the bookkeeping takes a campaign lock (`<campaign>/.dprod.lock`).
  A manual `submit` while `watch` runs waits for its turn, so the same task can't be submitted twice.
  It uses POSIX locks on the shared filesystem, so it also works across nodes (e.g. a scrontab round vs. sdfiana).

## Monitoring page

One page, `<web dir>/index.html`, with two data sources in the same format and style:

| source | file | written by | shows |
|---|---|---|---|
| **Live** | `status.json` | the controller: `dprod web`, or every round of `dprod watch --web` | the bookkeeping database synced with slurm: exact queued/running/failed |
| **Job records** | `jobs.json` | **the jobs themselves**, when they finish | the state as of the last finished job, with **no monitoring process** needed |

The page loads both, shows the more recent one (**Auto**), and says which one it is showing and how old each is.
**Live** / **Job records** switch it by hand. So if `watch` stops (session died, node rebooted),
the page keeps updating from the job records, and the stale live data is flagged.

How the job-side view works:
* **Records:** every attempt writes a small record, `<campaign>/records/<stage>/<task>_aNN.json`, when it starts
  and again when it ends. Every `dprod` command that changes bookkeeping writes `records/plan.json` (tasks
  per stage, abandoned tasks).
* **Rebuild:** a finishing job rebuilds `jobs.json` from these files and the submission manifests. This is rate
  limited to once per `job_rebuild_s` (300 s), but the last job to finish always rebuilds. Only one job rebuilds at a
  time. A problem there never fails the job.
* **Limits:** slurm's side is invisible to it. A task submitted but not started counts as
  queued. A task killed without writing its final record (OOM kill, node failure) stays "running"
  until it's past its slurm time limit, then shows as **lost**. `dprod sync` / `status` gives
  the exact picture.

Both views show:
* **Headline numbers:** events generated, fully processed events, running/queued, failed, data on disk.
* **Progress bars per stage:** done / running / queued / failed / lost / abandoned / not submitted.
* **Cumulative events over time**, per stage.
* **Per-stage statistics:** wall time, RAM, GPU utilization/memory, failure rate.
* **Wall-time histograms** and **the latest failures** with reasons.

Served over http(s), an open page re-fetches both JSON files every minute or two and redraws in place.
Opened as a local file, it shows the snapshot embedded by whoever wrote it last.

**Several campaigns.** Every campaign page registers itself in `<web base>/campaigns.json`. The
controller does this, and so do the jobs for the job-records view. `<web base>/index.html` is an
**All campaigns** overview: each campaign with its events, per-stage progress bars and failures, most recent first.
Every campaign page has a **Campaign** drop-down and an **All campaigns** link. `dprod destroy` removes the campaign
from the list. The links are relative, so they work under any web server. The web base is:

| `web.dir` in the site config | campaign pages | web base (overview, campaigns.json) |
|---|---|---|
| not set (default) | `<storage_root>/<campaign>/web/` | `<storage_root>/` |
| `/some/path/{campaign}` | `/some/path/<campaign>/` | `/some/path/` |
| `/some/path` (no `{campaign}`) | `/some/path/<campaign>/` | `/some/path/` |

To look at it before a web server is set up, serve the web base from sdfiana and tunnel to it:
```bash
cd <web base> && python3 -m http.server 8765                    # on sdfiana
ssh -L 8765:localhost:8765 <user>@<the same sdfiana node>        # on your laptop; open http://localhost:8765/
```

Setup in the site config:
```yaml
web:
  dir: /path/served/by/a/web/server/{campaign}   # default: <campaign>/web
  refresh_s: 600          # expected live update interval; older live data is flagged stale
  job_rebuild_s: 300      # jobs rebuild jobs.json at most this often
  job_snapshot: true      # set false to turn the job-side view off
  publish: "rsync -a {dir}/ user@host:/var/www/doraemon/{campaign}/"   # optional
```
For the job-side view, `web.dir` must be a directory that the **compute nodes can write** and a web
server serves. `publish` runs only from the controller, so with a remote copy the job-side updates
would not reach the web server.

## Re-processing another campaign's output (derived campaigns)

To re-run later stages on an existing campaign's output, for example stage 3 after a pysupera update, create
a campaign that **inherits** the earlier stages:
```yaml
campaign: prod_doraemon_2026_v0.0_supera2
inherit:
  campaign: prod_doraemon_2026_v0.0        # same site
  stages: [jaxtpc_wire, jaxtpc_pixel]      # their ancestors (edepsim) come along
stages:
  supera_wire:  {alias: 3A, parent: jaxtpc_wire, ...}    # the stages to run here
  supera_pixel: {alias: 3B, parent: jaxtpc_pixel, ...}
```
`configs/campaigns/example_derived_supera.yaml` is a complete example; start it with `dprod init`.
* **Read-only inheritance:** the inherited stages' definitions are resolved from the source campaign at `init`. Their
  tasks, files and (job, event) records are imported from its bookkeeping, pointing at its files; nothing is
  copied. They can't be submitted, extended or marked here, and `status` marks them with `*`.
* **Following the source:** every sync imports what the source has finished since, so a derived campaign can run
  while the source is still producing. Its own tasks become ready as the source's parent tasks finish. The source
  must be synced itself (its `watch`/cron does that).
* **Provenance and lookup:** outputs keep the full provenance chain (e.g. `edepsim -> jaxtpc_wire -> supera_wire`),
  with the derived campaign's config current and the source's under `campaign_config_history`. `lookup` finds the
  source's files and the new ones.
* **Destroy:** `dprod destroy` of a derived campaign deletes only its own files, never the source's.

The alternative is to stay within one campaign: add a stage (e.g. `supera_wire_v2` with parent `jaxtpc_wire`) with
`dprod update-config`.

## Destroying a campaign

```bash
bin/dprod destroy --dry-run            # what would be deleted (paths, sizes) and active jobs
bin/dprod destroy                      # asks you to type the campaign tag
bin/dprod destroy --confirm <tag>      # non-interactive
```
It cancels the campaign's queued and running jobs first and waits until they have stopped, so dying jobs don't write
files back. Then it deletes:
* the campaign directory: data, database, summaries, logs, records, code and inputs snapshots, and the default web page;
* the campaign's slurm log directory;
* a separate web directory, but only if its path contains the campaign tag (`web.dir: .../{campaign}`).
  A shared web directory is kept.

Each path is checked to belong to that campaign before anything is removed. Tags starting with `prod_` also need
`--allow-production`. Remove the campaign from your scrontab entry too; `destroy` reminds you if it's listed there.
After a destroy, the tag can be initialized again.

## Installation test (`bin/dprod-install-test`)

This runs one small job through **1 → 2A → 3A → 2B → 3B** on the current node, without slurm.
It creates a throwaway 1-job campaign from the real campaign config and runs each stage through the
production worker, in that stage's container. So it tests exactly what production runs: commands,
containers, environment, id checks, provenance and resource monitoring. Run it on a GPU node, since
JAXTPC needs one:

```bash
srun -p ampere --gpus 1 -c 8 --mem 64G -A <account> --pty bash     # an interactive GPU node
cd /sdf/group/neutrino/kterao/sw/doraemon/production
bin/dprod-install-test --site s3df --events 5 --outdir /sdf/data/neutrino/doraemon/install_test_$(date +%m%d)
```

Containers:
* By default, each stage uses the image the site config assigns it.
* `--image X` sets one image for every stage.
* `--image1`, `--image2` and `--image3` override stage 1 (edep-sim), 2A/2B (JAXTPC) and 3A/3B (pysupera).
  They take precedence over `--image`. For example, `--image test.sif --image1 larcv2.sif`.

Other options:
* `--stages 1,2A,3A` runs a subset.
* `--config` tests another campaign config.
* `--container-exec` / `--gpu-flags=--nv` override the container command. For example, on a
  machine without the site's bind paths:
  `--container-exec "apptainer exec {flags} -B /home {image}"`.

Output, organized per stage for inspection:
```
<outdir>/
  1_edepsim/        edepsim_j000000-000000.h5, logs/ (macro + yaml as executed, edep-sim log), slurm.log
  2A_jaxtpc_wire/   sensor/ step/ hits/ ..., logs/, slurm.log
  3A_supera_wire/   supera_wire_j000000-000000.h5, logs/, slurm.log
  2B_jaxtpc_pixel/  ...
  3B_supera_pixel/  ...
  campaign/         the throwaway campaign (bookkeeping db, summaries, data files)
  REPORT.txt        per stage: status, image, wall time, RAM, GPU; per file: size, (job, event)
                    ids, provenance chain (e.g. edepsim->jaxtpc_wire->supera_wire), software versions
```
It exits 0 only if every stage succeeded and every file check passed.

## Automated production with scrontab (S3DF)

`bin/dprod-cron` does one round per campaign: sync with slurm, submit everything that is ready in every
enabled stage (plus retries), update the monitoring page and the summary HDF5 files. Scheduled with
`scrontab`, each round is a short slurm job, so nothing has to stay logged in.

1. Check once that python on a compute node has PyYAML (install it for yourself if not):
   ```bash
   srun -p milano -A mli:nu-ml-dev -t 5 python3 -c "import yaml; print('ok')" || python3 -m pip install --user pyyaml
   ```
2. `scrontab -e` and add (the `#SCRON` lines apply to the entry below them):
   ```
   #SCRON --partition=milano
   #SCRON --account=mli:nu-ml-dev
   #SCRON --time=00:30:00
   #SCRON --cpus-per-task=1
   #SCRON --mem=4G
   #SCRON --job-name=dprod-cron
   #SCRON --output=/sdf/group/neutrino/doraemon/joblog/dprod-cron.log
   #SCRON --open-mode=append
   */15 * * * * /sdf/group/neutrino/kterao/sw/doraemon/production/bin/dprod-cron --site s3df test_doraemon_2026_smoke_v0.0
   ```
   * **More campaigns:** list several on the same line.
   * **Extra `watch` options:** add them after `--`, e.g. `... test_doraemon_2026_smoke_v0.0 -- --max-queued 300`,
     or `-- --partition roma --account X` to route all new submissions elsewhere.
   * **QOS:** don't use a preemptable QOS for this job.
3. `scrontab -l` shows the table, `squeue -u $USER -n dprod-cron` shows the scheduled job, and the log above
   shows every round.

Rounds can overlap with each other or with your manual `dprod` commands on sdfiana. They exclude each other
through the campaign lock, which uses POSIX locks on the shared filesystem and works across nodes. A round never
double-submits. To stop: `scrontab -e` and comment the entry out. Jobs already submitted keep running, and
the job-records view of the monitoring page keeps updating without the cron.

## Changing the configuration of a running campaign

The campaign config is frozen into `<campaign>/campaign.yaml` at `init`. To change it later:
```bash
bin/dprod update-config configs/campaigns/<campaign>.yaml --dry-run   # show the changes
bin/dprod update-config configs/campaigns/<campaign>.yaml             # apply (previous version kept)
```
* **Refused once the stage has run:** changes to what defines tasks and job/event ids: `events_per_job`,
  stage-1 inputs (`input_dir`, `geometry`, `macro`, `generator_config`), `merge`, `parent` and `handler`.
  The master `seed` is refused once anything has run. Removing a stage is refused too (set `enabled: false` instead).
  Each refusal says why.
* **Allowed:** everything else, e.g. enabling a stage, commands, `vars`, `env`, `slurm`, `provenance`,
  `max_attempts` or `max_queued`. It applies to submissions from then on. New stages get their tasks defined.
  `n_jobs` is ignored; use `dprod extend`.
* **Provenance:** outputs record which campaign-config version produced them. Every task block has
  `@campaign_config_sha256`, and earlier versions an input was made with are kept under
  `/provenance/campaign_config_history/`.
* **Code:** if the new config needs newer worker code (e.g. a new stage driver), also run `bin/dprod refresh-code`.

## Re-routing queued jobs (other partition / account)

**Option 1: move them in place.** No resubmission, no attempt used, and queue age is kept:
```bash
dprod move 1 --partition roma --account neutrino:other     # all queued stage-1 elements
dprod move 2A --tasks 40-59 --qos normal --time 03:00:00   # some of them
```
It uses `scontrol update` on the pending elements. It covers the partition, account, QOS and time limit.
For anything else (GPUs, memory, constraint), use option 2. Each move is logged in `<campaign>/submissions/moves.log`.

**Option 2: cancel them and resubmit with overrides.**
```bash
dprod cancel 1 --queued                                    # only pending elements; running ones keep running
dprod submit 1 --partition roma --account neutrino:other   # or: dprod advance / watch with the same flags
```
* **Cancel:** the cancelled tasks go back to *new*, and a cancellation never counts toward `max_attempts`. Without
  `--queued`, `cancel` also stops running elements, which then return to *new* the same way. This holds however
  the job was cancelled, including a plain `scancel`: slurm state CANCELLED is never counted as a failure.
  `submit`, `recover` and `advance` all resubmit such tasks.
* **Reset retries:** `dprod reset-attempts <stage> [--tasks 3,7-9]` starts the `max_attempts` count afresh for
  failed tasks, while keeping their attempt history. Then `dprod recover <stage>`. `recover --force` instead ignores the
  limit for one resubmission.
* **Overrides:** `submit`, `recover`, `advance` and `watch` all take `--partition`, `--account`, `--qos` and `--time`,
  plus `--slurm KEY=VALUE` for any other sbatch option (e.g. `--slurm mem=64G --slurm constraint=a100`).
  They apply to those submissions only; the site config is unchanged. Each manifest records the overrides it used.
* **For good:** to change it permanently, edit the profile in `configs/sites/<site>.yaml`. The next submission uses it.

## Running at S3DF (first test)

1. **Put the repo on S3DF**, somewhere under `/sdf` so that jobs can see it:
   ```bash
   rsync -a --exclude __pycache__ production/ s3dflogin.slac.stanford.edu:/sdf/group/neutrino/kterao/sw/doraemon/production/
   ```
2. **Log in to an interactive node** (`ssh s3dflogin` → `ssh sdfiana`). This is where `dprod` runs: it needs
   `sbatch`/`sacct`, `apptainer` (for `merge-summary`), and python ≥ 3.6 with PyYAML (the system python3 on sdfiana is fine)
   (`python3 -c "import yaml"`; if that fails, `pip install --user pyyaml`).
3. **Edit `configs/sites/s3df.yaml`** where marked TODO: `images.stage23` (path of test.sif),
   `vars.jaxtpc_dir`, the edep-sim/DLPGenerator paths in `env`/`vars`, and the accounts.
   The GPU profile uses `mli:cider-ml`. Use an account you can charge.
4. **Preflight**, which reports every problem at once. It also starts each stage's container here and imports
   what the worker and the stage need (`check_imports` per stage), so an unreadable image or a missing
   `python3`/`jax`/`pysupera` shows up before any submission. `--no-images` skips that part:
   ```bash
   cd /sdf/group/neutrino/kterao/sw/doraemon/production
   bin/dprod --site s3df check configs/campaigns/test_doraemon_2026_smoke_v0.0.yaml
   ```
5. **Smoke test** (4 jobs × 5 events, stage 2 merges 2 jobs per task):
   ```bash
   bin/dprod init                    # choose s3df and the smoke config; it becomes the current campaign
   bin/dprod submit 1 --dry-run      # look at the generated sbatch script first
   bin/dprod submit 1
   bin/dprod status                  # squeue -u $USER also works
   bin/dprod failures 1 -v           # if anything failed: reason + log tarball
   bin/dprod advance                 # submits 2A/2B tasks whose stage-1 jobs are done,
                                     # then 3A/3B tasks whose JAXTPC tasks are done
   bin/dprod watch --recover         # or: let it run everything to the end
   bin/dprod tasks 2A                # per-task wall time, RAM, GPU
   bin/dprod merge-summary           # summary HDF5 files in the campaign dir
   bin/dprod lookup 1 3              # files holding job 1, event 3
   ```
   Where things end up:
   * Campaign dir: `/sdf/data/neutrino/doraemon/test_doraemon_2026_smoke_v0.0/`. It holds `data/`,
     `summaries/`, `logs/`, `bookkeeping.sqlite`, and `*_summary.h5`.
   * Slurm logs: `/sdf/group/neutrino/doraemon/joblog/test_doraemon_2026_smoke_v0.0/<stage>/`.
   * Provenance: `python3 -m doraemon_prod.provenance <file>`, run inside the test.sif container.
6. **To redo a smoke test**, either delete its campaign directory, or change the `campaign:` tag
   in the config (e.g. `..._v0.1`). A tag can't be initialized twice.

Changes to `doraemon_prod/` after `init` reach jobs only after `bin/dprod refresh-code`, because
workers run the snapshot in `<campaign>/code/`. Site-config changes (accounts, paths) apply to the next
submission immediately. The campaign config is frozen at `init`.

## Job summary HDF5

Each successful stage-1 attempt writes `summaries/edepsim/<task>_aNN.h5`.
`dprod merge-summary` appends newly finished jobs to one file per campaign,
`<campaign>_edepsim_summary.h5`. `--rebuild` rewrites it, and it rebuilds automatically if a
merged job's attempt is no longer the current one.

| dataset | one row per | columns |
|---|---|---|
| `/job` | job | job_id, attempt, n_events, start_time (unix), duration_s, node, max_rss_mb, avg_rss_mb, gpu_util_pct, gpu_mem_used_mb (NaN for CPU jobs), seed_geant4, seed_generator, event_start/event_end (fence post into `/event`) |
| `/event` | event | job_id, event_id, particle_start/particle_end (fence post into `/particle`), num_vertices, num_primaries, num_particles (all Geant4), n_proton, n_pion_charged, n_kaon_charged, n_pion0, n_neutron, n_electron, n_positron, n_muon, n_antimuon, n_photon (primaries), primary_ke_sum [MeV], num_segments |
| `/particle` | primary particle | job_id, event_id, interaction_id, track_id, pdg, x, y, z [mm], t [ns], E, px, py, pz [MeV] |

Downstream stages (JAXTPC, and later pysupera) get `<campaign>_<stage>_summary.h5`, which holds a single
`/job` table with one row per task. `merge-summary` rebuilds it from the workers' JSON summaries every time:

| column | meaning |
|---|---|
| task_id, attempt, first_job, last_job | task, its successful attempt, and the stage-1 job range it processed |
| n_input_jobs, n_input_events, n_events | stage-1 jobs / events given as input; events in the output |
| n_files, output_bytes | registered output files and their total size |
| start_time, duration_s, node, slurm_job_id | when/where it ran |
| max_rss_mb, avg_rss_mb | peak / time-averaged CPU RAM |
| n_gpus, gpu_util_pct, gpu_mem_used_mb, gpu_mem_max_mb, gpu_mem_total_mb | time-averaged GPU utilization and memory, peak memory, card memory (NaN if not a GPU job) |
| seed | the task seed passed as `{seed}` |

`dprod merge-summary [stage,...]` writes every stage by default.

A primary's 4-position is the creation point of its interaction: the
`vertex/geant4` row whose interaction_id matches the primary's. E = ke + mass. Jobs are appended in completion order, so use `/job` to find a job.
Only the first `attrs["n_job"/"n_event"/"n_particle"]` rows are committed, which
makes an interrupted merge harmless: the next merge truncates the extra rows.

## Provenance in the output files

Every output file carries the configuration that produced it, in a top-level
`/provenance` group (`doraemon_prod/provenance.py`). **Each stage appends its own
block.** A downstream stage first copies the provenance of the input file(s)
the output was made from, so a file carries the whole chain back to the generator:

```
/provenance/              @campaign  @stages=['edepsim', 'jaxtpc_wire', ...]  @format_version
  campaign_config/        campaign.yaml [text] (@path @sha256) + config/ (as a dictionary); once per file
  edepsim/                stage block: @stage @alias @handler
    stage_config/         options set for this stage in the campaign config
    j000003-000003/       task block (the job range the task processed):
                            @task_id @attempt @first_job @last_job @host @slurm_job_id
                            @created @image @command @dprod_version
      seeds/              @task @geant4 @generator
      files/              geometry, macro, generator_config [text, as executed]
                          (@<name>.path, @<name>.sha256 on files/)
      config/             generator_config as a dictionary
      software/           edep-sim (@path @sha256), edep-sim_src, DLPGenerator (@git_commit @git_dirty ...)
      container/          the image the job ran in: @runtime @path @size @mtime_iso @build_date
                          @head_tail_sha256 (fingerprint: size + first/last 4 MiB, cheap for multi-GB images)
                          @definition_sha256, labels/ (base image, version, ...); the definition file
                          itself is files/container_definition
      environment/        @PATH @LD_LIBRARY_PATH ...
      inputs              [string array]
  jaxtpc_wire/            stage block
    stage_config/
    j000002-000003/       task block: files/ + config/ detector_config, production_config;
                          software/JAXTPC; inputs = its edep-sim files; seeds/@task
  supera_wire/ ...        (stage 3, appended the same way)
  _blobs/<sha256>         the texts; files/<name> are hard links, so identical texts are stored once
```

* A stage block can hold several task blocks. For example, a stage-3 file merging jobs 0–3
  holds `edepsim/j000000-000000` … `j000003-000003`, plus the one or two `jaxtpc_wire`
  task blocks that processed those jobs. A JAXTPC sensor file holds exactly one
  `edepsim` block, the one for its own source job. The upstream blocks are chosen by the jobs actually
  present in the output file.
* Text is deduplicated within a file: a large GDML shared by N merged jobs is stored once. The
  per-job macro and generator YAML differ because each contains that job's seeds.
* **Container:** every task block records the image it actually ran in, as seen from inside the job. That's the image
  path apptainer reports, its size and modification time, and a head/tail SHA-256 fingerprint, so a rebuilt image
  with the same name can be told apart. Also the labels stored in the image (build date, base image) and its
  definition file. With shifter, the image name. Job summaries carry the same information, and
  `dprod-install-test` shows it per file.
* **Stage 1 (edep-sim)** gets these automatically: the geometry, the macro and the generator YAML *as
  executed* (with the per-job seeds and run id), and the edep-sim binary. Source checkouts
  come from the stage's `provenance.software` (`{edepsim_dir}`, `{dlpgen_dir}` in the site config).
* **Downstream stages** list what to record in the campaign config. For JAXTPC it goes into the sensor files
  only (`step/`, `hits/` get none):

  ```yaml
  vars:                                    # shared by command and provenance
    detector_config: "{jaxtpc_dir}/config/cubic_wireplane_config.yaml"
    production_config: "{jaxtpc_dir}/config/production_cubic_wireplane_doraemon_300micro.yaml"
  command: ... --config {detector_config} --production-config {production_config} ...
  provenance:
    roles: [sensor]                        # omit = every output file
    files: {detector_config: "{detector_config}", production_config: "{production_config}"}
    software: {JAXTPC: "{jaxtpc_dir}"}
  ```
  A listed file that doesn't exist fails the task, so provenance is never silently incomplete. A
  downstream stage finds its parents' provenance in the parent files of the parent's
  `provenance.roles` (e.g. stage 3 reads it from the JAXTPC sensor files).

The dictionary encoding: dict → group, scalar → attribute, list of numbers or strings → array attribute,
list of dicts → subgroups `0`, `1`, …, and None → empty attribute. The raw text is always stored too.
To read it back:

```python
from doraemon_prod.provenance import read_provenance, task_blocks
p = read_provenance("sim_wire_sensor_0000_00.h5")
p["stages"]                                   # ['edepsim', 'jaxtpc_wire']
(e,) = task_blocks(p, "edepsim")              # the edep-sim job this file came from
e["seeds"], e["files"]["geometry"], e["config"]["generator_config"]["Generator1"]
task_blocks(p, "jaxtpc_wire")[0]["config"]["detector_config"]
```
or `python3 -m doraemon_prod.provenance <file.h5> [--files]`, which prints JSON.

## Resource monitoring

Every job runs a sampler thread (every `monitor_interval` s, default 10) that records:
* CPU RAM: the summed RSS of the worker's process tree. `avg_rss_mb` is its
  time average over the job. `max_rss_mb` is the larger of the sampled peak and the
  kernel's peak for the biggest single process.
* GPU jobs only: `nvidia-smi` utilization.gpu and memory.used of the GPUs the job
  was given (`CUDA_VISIBLE_DEVICES`), time-averaged, plus the peak memory. A stage is treated as a GPU stage if its slurm
  options request GPUs, or if `monitor_gpu: true` is set.

The values go into the attempt's JSON summary, the database (`dprod status`, `dprod tasks <stage>`),
and the stage-1 job summary HDF5.
Note: JAX preallocates 75% of GPU memory by default, so the GPU memory numbers for
JAXTPC measure that pool, not actual use, unless `XLA_PYTHON_CLIENT_PREALLOCATE=false` is set.

## Validation done by the worker

* edep-sim exits 0 even when a macro command aborts, so its log is also scanned for
  `COMMAND NOT FOUND`, `Batch is interrupted`, etc.
* Every output file is opened and its `(job, event)` ids are read (`idreaders.py`):
  stage 1 must hold exactly events `0..N-1` of its job; downstream files must stay within the task's job range and have no duplicates.
  If event counts differ from the inputs, that's a warning, or a failure with `strict_events: true`.
* Outputs are copied to storage under a temp name and renamed atomically. A missing
  summary after the scheduler reports a terminal state (TIMEOUT, OOM, PREEMPTED, ...) is a failure.

## Layout

```
configs/sites/{s3df,nersc,local}.yaml   paths, container runtime, images, slurm accounts/partitions
configs/campaigns/*.yaml                stages, job counts, merge factors, commands
doraemon_prod/                          controller (cli, campaign, db, scheduler, report, config)
                                        + worker side (worker, idreaders, layout), stdlib + h5py only
templates/sbatch.sh.in                  generated array script
configs/edepsim/                        stage-1 inputs (geometry, macro, generator yaml); a campaign
                                        selects them with input_dir (add setups as sibling directories)
tests/run_local_test.sh                 end-to-end test with the local scheduler
```

Campaign directory: `campaign.yaml` (frozen config), `bookkeeping.sqlite`,
`code/` (worker code snapshot; `dprod refresh-code` to update),
`inputs/<stage>/`, `submissions/<stage>/sub_NNNNN.{json,sh}`,
`summaries/<stage>/`, `logs/<stage>/*.tgz`, `data/<stage>/jNNNNNN/` (shards of 1000 jobs).

## Testing locally

```bash
tests/run_local_test.sh /tmp/dprod_test     # on the host; needs apptainer + an NVIDIA GPU
```

This runs 6 real edep-sim jobs (3 events each) in the stage-1 image, and a fake JAXTPC
stage in `test.sif`. The fake stage mimics JAXTPC's layout, loads the GPU with JAX and fails once on
purpose to test recovery. `tests/check_summary.py` then cross-checks the merged job summary
against the edep-sim files.
