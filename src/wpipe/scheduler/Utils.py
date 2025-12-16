import os
import signal
import logging
import json
from datetime import datetime
from typing import Tuple, Callable


def has_pbs_or_slurm() -> Tuple[bool, bool]:
    has_pbs = os.system("which qsub") == 0
    has_slurm = os.system("which sbatch") == 0

    if has_pbs and has_slurm:
        print("WARNING: Found both PBS and Slurm... continuing with PBS...")

    return has_pbs, has_slurm


def no_function_returned_related_to_scheduler() -> None:
    raise RuntimeError(
        "Wasn't able to give a consumer when were expected to use one ...\n "
        "please define the WPIPE_NO_SCHEDULER environment variable for no scheduler"
    )


def save_failed_job(consumer_type: str, job_data, prefix: str = "job") -> str:
    """
    Save a failed job to the failed_jobs directory.

    This is a shared utility function used by both connection failure handling
    and signal termination handling to persist job state for later recovery.

    Parameters
    ----------
    consumer_type : str
        Type of consumer ("pbs" or "slurm")
    job_data : JobData
        Job data object with to_dict() method
    prefix : str, optional
        Filename prefix (default: "job"). Use "signal_job" for signal terminations.

    Returns
    -------
    str
        Path to the saved job file

    Raises
    ------
    Exception
        If job cannot be saved
    """
    failed_jobs_dir = os.path.expanduser(
        "~/.{}consumer/failed_jobs".format(consumer_type)
    )
    os.makedirs(failed_jobs_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    failed_job_path = os.path.join(
        failed_jobs_dir, "{}_{}.json".format(prefix, timestamp)
    )

    with open(failed_job_path, "w") as f:
        json.dump(job_data.to_dict(), f, indent=2)

    return failed_job_path


def save_pending_jobs_on_signal(consumer_name: str) -> int:
    """
    Save all pending jobs from active schedulers to failed_jobs directory.

    Called by signal handler to preserve job state when consumer terminates
    unexpectedly. Iterates through all active PBS/Slurm schedulers and saves
    any queued jobs that haven't been submitted yet.

    Parameters
    ----------
    consumer_name : str
        Name of the consumer ("PbsConsumer" or "SlurmConsumer")

    Returns
    -------
    int
        Number of jobs saved
    """
    jobs_saved = 0

    try:
        # Determine scheduler and consumer type based on consumer name
        if consumer_name == "PbsConsumer":
            consumer_type = "pbs"
            try:
                from .PbsScheduler import PbsScheduler

                schedulers = PbsScheduler.schedulers
            except ImportError:
                logging.error("Failed to import PbsScheduler")
                return 0
        elif consumer_name == "SlurmConsumer":
            consumer_type = "slurm"
            try:
                from .SlurmScheduler import SlurmScheduler

                schedulers = SlurmScheduler.schedulers
            except ImportError:
                logging.error("Failed to import SlurmScheduler")
                return 0
        else:
            logging.error("Unknown consumer name: %s", consumer_name)
            return 0

        # Save jobs from all active schedulers
        for scheduler in schedulers:
            if hasattr(scheduler, "_jobList"):
                for jobdata in scheduler._jobList:
                    try:
                        failed_job_path = save_failed_job(
                            consumer_type, jobdata, prefix="signal_job"
                        )
                        jobs_saved += 1
                        logging.info("Saved pending job to %s", failed_job_path)
                    except Exception as e:
                        logging.error("Failed to save job: %s", str(e))

        if jobs_saved > 0:
            logging.warning(
                "Saved %d pending job(s) due to signal termination", jobs_saved
            )
        else:
            logging.info("No pending jobs to save")

    except Exception as e:
        logging.error("Error saving pending jobs: %s", str(e))

    return jobs_saved


def create_signal_handler(consumer_name: str) -> Callable:
    """
    Create a signal handler for consumer processes.

    This handler logs detailed information about signals received before
    allowing default signal behavior to proceed.

    Parameters
    ----------
    consumer_name : str
        Name of the consumer (e.g., "SlurmConsumer", "PbsConsumer")

    Returns
    -------
    Callable
        Signal handler function

    Examples
    --------
    >>> signal_handler = create_signal_handler("SlurmConsumer")
    >>> signal.signal(signal.SIGTERM, signal_handler)
    """

    def signal_handler(signum, frame):
        """Log signal reception and continue with default behavior."""
        signal_name = signal.Signals(signum).name
        pid = os.getpid()
        ppid = os.getppid()

        logging.warning(
            "%s: Received signal %s (%d). Consumer process is terminating.",
            consumer_name,
            signal_name,
            signum,
        )
        logging.info("Process ID: %d, Parent Process ID: %d", pid, ppid)

        # Log frame info if available
        if frame:
            try:
                logging.info(
                    "Signal frame info: %s:%d", frame.f_code.co_filename, frame.f_lineno
                )
            except AttributeError:
                logging.info("Signal frame info: unavailable")

        # Try to get parent process name (Linux-specific via /proc)
        try:
            # Check if /proc filesystem is available
            if os.path.isdir("/proc"):
                comm_file = "/proc/{}/comm".format(ppid)
                if os.path.exists(comm_file):
                    with open(comm_file, "r") as f:
                        parent_name = f.read().strip()
                    logging.info("Parent process name: %s", parent_name)

                    # Also try to get command line for more context
                    cmdline_file = "/proc/{}/cmdline".format(ppid)
                    if os.path.exists(cmdline_file):
                        try:
                            with open(cmdline_file, "r") as f:
                                cmdline = f.read().replace("\x00", " ").strip()
                            logging.info("Parent command line: %s", cmdline)
                        except (IOError, OSError):
                            pass
                else:
                    logging.info("Parent process %d no longer exists", ppid)
            else:
                logging.info("Parent process info unavailable (/proc not available)")
        except (FileNotFoundError, PermissionError, OSError) as e:
            logging.info("Could not determine parent process info: %s", e)
        except Exception as e:
            logging.warning("Unexpected error getting parent info: %s", e)

        # Note: Standard signal handlers don't provide sender PID
        logging.info(
            "Note: Sender process information not available via signal handler. "
            "Check system logs (journalctl, dmesg) for sender details."
        )

        # Save any pending jobs before terminating
        logging.info("Attempting to save pending jobs...")
        try:
            jobs_saved = save_pending_jobs_on_signal(consumer_name)
            if jobs_saved > 0:
                logging.warning(
                    "Successfully saved %d pending job(s) before termination",
                    jobs_saved,
                )
        except Exception as e:
            logging.error("Failed to save pending jobs: %s", str(e))

        # Restore default handler and re-raise signal for default behavior
        try:
            signal.signal(signum, signal.SIG_DFL)
            os.kill(pid, signum)
        except Exception as e:
            logging.error("Failed to restore signal handler: %s", str(e))
            # Force exit if we can't re-raise the signal
            os._exit(128 + signum)

    return signal_handler


def setup_signal_handlers(consumer_name: str) -> None:
    """
    Set up signal handlers for a consumer process.

    Registers handlers for common termination and resource limit signals
    that log detailed information before terminating. Includes HPC-specific
    signals for supercomputing cluster environments.

    Parameters
    ----------
    consumer_name : str
        Name of the consumer (e.g., "SlurmConsumer", "PbsConsumer")

    Examples
    --------
    >>> setup_signal_handlers("SlurmConsumer")
    """
    handler = create_signal_handler(consumer_name)

    # Register handlers for common termination signals
    signal.signal(signal.SIGTERM, handler)  # Termination signal
    signal.signal(signal.SIGINT, handler)  # Interrupt (Ctrl+C)
    signal.signal(signal.SIGHUP, handler)  # Hangup
    signal.signal(signal.SIGQUIT, handler)  # Quit
    signal.signal(signal.SIGABRT, handler)  # Abort (from abort() or assertions)
    signal.signal(signal.SIGPIPE, handler)  # Broken pipe
    signal.signal(signal.SIGALRM, handler)  # Alarm/timeout

    # Register handlers for HPC resource limit signals
    signal.signal(signal.SIGXCPU, handler)  # CPU time limit exceeded
    signal.signal(signal.SIGXFSZ, handler)  # File size limit exceeded

    # Register handlers for job scheduler signals
    signal.signal(signal.SIGUSR1, handler)  # User-defined signal 1
    signal.signal(signal.SIGUSR2, handler)  # User-defined signal 2

    logging.info(
        "%s: Signal handlers registered for SIGTERM, SIGINT, SIGHUP, SIGQUIT, "
        "SIGABRT, SIGPIPE, SIGALRM, SIGXCPU, SIGXFSZ, SIGUSR1, SIGUSR2",
        consumer_name,
    )
