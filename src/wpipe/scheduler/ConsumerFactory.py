from typing import Callable, Tuple
import os

from src.wpipe.scheduler.Utils import has_pbs_or_slurm, no_function_returned_related_to_scheduler
from . import pbsconsumer, sendJobToPbs
from . import slurmconsumer, sendJobToSlurm


def get_consumer_factory() -> Callable:
    """
    We check for Slurm or PBS schedulers in the environment and return the associated consumer.  If the system
    has both we return the PBS consumer.
    """
    has_pbs, has_slurm = has_pbs_or_slurm()

    if has_pbs:
        return pbsconsumer
    if has_slurm:
        return slurmconsumer

    no_function_returned_related_to_scheduler()


def get_send_job_factory() -> Callable:
    has_pbs, has_slurm = has_pbs_or_slurm()

    if has_pbs:
        return sendJobToPbs
    if has_slurm:
        return sendJobToSlurm

    no_function_returned_related_to_scheduler()


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