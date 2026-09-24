"""DORAEMON production control and bookkeeping.

Controller side (login node): config, db, submit, sync, report, cli.
  Requires python>=3.6 with PyYAML; talks to slurm via sbatch/sacct.
Worker side (inside the job container): worker, idreaders.
  Requires only the standard library and h5py; reads a JSON manifest.
"""

__version__ = "0.1.0"
