import os
from typing import Tuple


def has_pbs_or_slurm() -> Tuple[bool, bool]:
    has_pbs = os.system("which qsub") == 0
    has_slurm = os.system("which sbatch") == 0

    if has_pbs and has_slurm:
        print("WARNING: Found both PBS and Slurm... continuing with PBS...")

    return has_pbs, has_slurm

def no_function_returned_related_to_scheduler() -> None:
    raise RuntimeError("Wasn't able to give a consumer when were expected to use one ...\n "
                       "please define the WPIPE_NO_SCHEDULER environment variable for no scheduler")
