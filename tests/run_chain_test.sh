#!/bin/bash
# End-to-end test with the REAL stage 2/3 software (JAXTPC, pysupera) and the
# local scheduler: event identity and merged outputs across all stages, then a
# derived campaign re-labelling stage 1 through HDF5 links (stages.link_edepsim).
# Run on the host (needs apptainer and an NVIDIA GPU):
#   tests/run_chain_test.sh <scratch dir>
# Uses the JAXTPC and pysupera checkouts named in configs/sites/local.yaml
# (jaxtpc_dir, pysupera_dir). Takes ~30 min, mostly JAXTPC's JIT compilation.
set -eo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
[ -n "$1" ] || { echo "usage: $0 <scratch dir>"; exit 2; }
ROOT="$(readlink -f "$1")"
IMG1=${IMG1:-/home/kazu/sw/images/larcv2_ub2204-cuda121-torch251-larndsim-2025-03-20.sif}
IMG23=${IMG23:-/home/kazu/sw/images/test.sif}
TAG=chaintest_doraemon_v0.0
rm -rf "$ROOT"; mkdir -p "$ROOT"
export DPROD_LOCAL_ROOT="$ROOT"
export DPROD_BATCH=1
export DPROD_STATE=""                         # do not touch ~/.config/dprod
cp "$HERE/configs/sites/local.yaml" "$ROOT/site.yaml"
_D() { img=$1; shift; echo "+ dprod $*"
       apptainer exec --nv -B /home/kazu,/tmp "$img" \
           "$HERE/bin/dprod" --site "$ROOT/site.yaml" --campaign $TAG "$@"; }
D1() { _D "$IMG1" "$@"; }       # stage-1 image (edep-sim)
D2() { _D "$IMG23" "$@"; }      # stage-2/3 image (JAX, h5py)
all_done() {                    # every task of the campaign in $1 is done
    apptainer exec -B /home/kazu,/tmp "$IMG23" python3 -c '
import sqlite3, sys
bad = sqlite3.connect(sys.argv[1] + "/bookkeeping.sqlite").execute(
    "SELECT stage, task_id, status FROM tasks WHERE status != ?", ("done",)).fetchall()
sys.exit("tasks not done: %s" % bad if bad else 0)' "$1"; }

D1 init "$HERE/tests/campaign_chain_test.yaml"
D1 submit 1
D2 submit 2A
D2 submit 2B
D2 submit 3A
D2 submit 3B
D2 status
D2 failures 3B || true
C="$ROOT/storage/$TAG"
D2 files 2A -l
D2 files 3B -l
all_done "$C"
D2 lookup --event-id 7                       # job 2, event 1: one file per stage and role
D2 audit $(find "$C/data/jaxtpc_wire" -name "*hits*.h5") $(find "$C/data/supera_pixel" -name "*.h5")
apptainer exec -B /home/kazu,/tmp "$IMG23" python3 "$HERE/tests/check_identity.py" "$C" 3

# derived campaign: stage-1 files without /dprod (as written before dprod wrote
# it), re-labelled through HDF5 external links (stages.link_edepsim), then 2A-3B
apptainer exec -B /home/kazu,/tmp "$IMG23" python3 -c '
import glob, sys, h5py
for p in glob.glob(sys.argv[1] + "/data/edepsim/*/*.h5"):
    with h5py.File(p, "a") as f:
        del f["dprod"]
' "$C"
TAG=chainderived_doraemon_v0.0
D2 init "$HERE/tests/campaign_chain_derived_test.yaml"
D2 submit 1L
D2 submit 2A
D2 submit 2B
D2 submit 3A
D2 submit 3B
D2 status
CD="$ROOT/storage/$TAG"
all_done "$CD"
D2 lookup --event-id 7
D2 audit $(find "$CD/data/edepsim_link" -name "*.h5" | head -1) $(find "$CD/data/supera_wire" -name "*.h5")
apptainer exec -B /home/kazu,/tmp "$IMG23" python3 "$HERE/tests/check_identity.py" "$CD" 3 chaintest_doraemon_v0.0
du -sh "$CD/data/edepsim_link"
echo "ALL DONE"
