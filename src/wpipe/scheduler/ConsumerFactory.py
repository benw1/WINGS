from typing import Callable
import os
from . import pbsconsumer
from . import slurmconsumer


def get_consumer_factory() -> Callable:
    """
    We check for Slurm or PBS schedulers in the environment and return the associated consumer.  If the system
    has both we return the PBS consumer.
    """
    has_pbs = os.system("which qsub") == 0
    has_slurm = os.system("which slurm") == 0

    if has_pbs and has_slurm:
        print("WARNING: Found both PBS and Slurm... continuing with PBS...")

    if has_pbs:
        return pbsconsumer
    if has_slurm:
        return slurmconsumer
    raise RuntimeError("Wasn't able to give a consumer when were expected to use one ...\n "
                       "please define the WPIPE_NO_SCHEDULER environment variable for no scheduler")


if __name__ == "__main__":
    # can run for a little check
    which_pbs = os.system("which qsub") == 0
    which_slurm = os.system("which slurm") == 0

    print(f"Results of slurm and PBS test: PBS {which_pbs} & Slurm {which_slurm} ...\n")

    if which_pbs and which_slurm:
        print("WARNING: Found both PBS and Slurm... continuing with PBS...")

    if which_pbs:
        print("We found PBS on this system ...")
    else:
        print("Didn't find PBS on this system ...")

    if which_slurm:
        print("We found Slurm on this system ...")
    else:
        print("Didn't find Slurm on this system ...")