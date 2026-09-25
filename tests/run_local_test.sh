#!/bin/bash
# End-to-end test with the local scheduler. Run on the host (needs apptainer and
# an NVIDIA GPU):   tests/run_local_test.sh <scratch dir>
# Each dprod call runs inside a container, as a slurm job would: stage-1 steps in
# the stage-1 image, stage-2 steps in the stage-2/3 image with --nv (the fake
# stage 2 loads the GPU with JAX to test resource monitoring).
set -eo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
[ -n "$1" ] || { echo "usage: $0 <scratch dir>"; exit 2; }
ROOT="$(readlink -f "$1")"
IMG1=${IMG1:-/home/kazu/sw/images/larcv2_ub2204-cuda121-torch251-larndsim-2025-03-20.sif}
IMG23=${IMG23:-/home/kazu/sw/images/test.sif}
rm -rf "$ROOT"; mkdir -p "$ROOT/fail"
export DPROD_LOCAL_ROOT="$ROOT"
export DPROD_BATCH=1
export DPROD_STATE=""                         # do not touch ~/.config/dprod                         # no confirmation prompts

# site config for the test: the local site + variables used by the fake stage 2
sed -e "s#^vars:#vars:\n  test_dir: $HERE/tests\n  fail_dir: $ROOT/fail#" \
    "$HERE/configs/sites/local.yaml" > "$ROOT/site.yaml"
_D() { img=$1; shift; echo "+ dprod $*"
       apptainer exec --nv -B /home/kazu,/tmp "$img" \
           "$HERE/bin/dprod" --site "$ROOT/site.yaml" --campaign localtest_doraemon_v0.0 "$@"; }
D1() { _D "$IMG1" "$@"; }       # stage-1 image (edep-sim)
D2() { _D "$IMG23" "$@"; }      # stage-2/3 image (JAX, h5py)
PY2() { apptainer exec -B /home/kazu,/tmp "$IMG23" python3 "$@"; }

D1 init "$HERE/tests/campaign_local_test.yaml"
touch "$ROOT/fail/fail_2"                    # stage-2 task covering jobs 2-3 fails once

D1 submit 1 --limit 5                        # 5 tasks -> arrays of 4 + 1 (max_array_size 4)
D2 submit 2A                                 # groups (0,1),(2,3) ready; (4,5) waits for job 5
D2 status
D2 merge-summary                             # 5 done jobs
D1 submit 1                                  # job 5
D2 submit 2A                                 # group (4,5)
D2 failures 2A
D2 recover 2A                                # task 1 (jobs 2-3) succeeds on attempt 2
D2 status
D2 lookup 0 2
D2 lookup 3 1
D2 files 2A -l
D2 tasks 1
D2 tasks 2A
D2 submit 3A || echo "(expected: stage 3A is disabled)"
D2 merge-summary                             # appends job 5
C="$ROOT/storage/localtest_doraemon_v0.0"
PY2 "$HERE/tests/check_summary.py" "$C/localtest_doraemon_v0.0_edepsim_summary.h5" "$C/data/edepsim"
PY2 "$HERE/tests/check_task_table.py" "$C/localtest_doraemon_v0.0_jaxtpc_wire_summary.h5" 3 6
PY2 "$HERE/tests/check_provenance.py" "$C" "$ROOT"
D2 merge-summary --rebuild
PY2 "$HERE/tests/check_summary.py" "$C/localtest_doraemon_v0.0_edepsim_summary.h5" "$C/data/edepsim" | tail -1
echo "ALL DONE"
