import os
import signal
import logging
from typing import Tuple, Callable


def has_pbs_or_slurm() -> Tuple[bool, bool]:
    has_pbs = os.system("which qsub") == 0
    has_slurm = os.system("which sbatch") == 0

    if has_pbs and has_slurm:
        print("WARNING: Found both PBS and Slurm... continuing with PBS...")

    return has_pbs, has_slurm


def no_function_returned_related_to_scheduler() -> None:
    raise RuntimeError("Wasn't able to give a consumer when were expected to use one ...\n "
                       "please define the WPIPE_NO_SCHEDULER environment variable for no scheduler")


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
            f"{consumer_name}: Received signal {signal_name} ({signum}). "
            f"Consumer process is terminating."
        )
        logging.info(f"Process ID: {pid}, Parent Process ID: {ppid}")

        # Log frame info if available
        if frame:
            try:
                logging.info(f"Signal frame info: {frame.f_code.co_filename}:{frame.f_lineno}")
            except AttributeError:
                logging.info("Signal frame info: unavailable")

        # Try to get parent process name (Linux-specific via /proc)
        try:
            # Check if /proc filesystem is available
            if os.path.isdir('/proc'):
                comm_file = f"/proc/{ppid}/comm"
                if os.path.exists(comm_file):
                    with open(comm_file, "r") as f:
                        parent_name = f.read().strip()
                    logging.info(f"Parent process name: {parent_name}")

                    # Also try to get command line for more context
                    cmdline_file = f"/proc/{ppid}/cmdline"
                    if os.path.exists(cmdline_file):
                        try:
                            with open(cmdline_file, "r") as f:
                                cmdline = f.read().replace('\x00', ' ').strip()
                            logging.info(f"Parent command line: {cmdline}")
                        except (IOError, OSError):
                            pass
                else:
                    logging.info(f"Parent process {ppid} no longer exists")
            else:
                logging.info("Parent process info unavailable (/proc not available)")
        except (FileNotFoundError, PermissionError, OSError) as e:
            logging.info(f"Could not determine parent process info: {e}")
        except Exception as e:
            logging.warning(f"Unexpected error getting parent info: {e}")

        # Note: Standard signal handlers don't provide sender PID
        logging.info(
            "Note: Sender process information not available via signal handler. "
            "Check system logs (journalctl, dmesg) for sender details."
        )

        # Restore default handler and re-raise signal for default behavior
        try:
            signal.signal(signum, signal.SIG_DFL)
            os.kill(pid, signum)
        except Exception as e:
            logging.error(f"Failed to restore signal handler: {e}")
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
    signal.signal(signal.SIGINT, handler)   # Interrupt (Ctrl+C)
    signal.signal(signal.SIGHUP, handler)   # Hangup
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
        consumer_name
    )
