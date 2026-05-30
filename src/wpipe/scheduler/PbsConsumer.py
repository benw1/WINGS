#!/usr/bin/env python
"""
Contains the PbsConsumer utilities including the scheduler.checkPbsConnection
and scheduler.sendJobToPbs function definitions

Please note that this module is private. These functions are available in the
main ``wpipe.scheduler`` namespace - use that instead.
"""

import asyncio
import pickle
import socket
import logging
import os
import sys
from datetime import datetime
from wpipe.scheduler.StreamToLogger import StreamToLogger
from wpipe.scheduler.JobData import JobData
from wpipe.scheduler.PbsScheduler import PbsScheduler
from wpipe.scheduler.Utils import setup_signal_handlers, save_failed_job
from wpipe.sqlintf import SESSION

__all__ = ["BASE_PORT", "DEFAULT_PORT", "checkPbsConnection", "sendJobToPbs"]

BASE_PORT = DEFAULT_PORT = 5000


def _get_pbs_address():
    """Return (host, port) from address file or defaults. Never cached."""
    from wpipe.scheduler.Utils import read_address_file
    result = read_address_file("pbs")
    if result is not None:
        host, port, _ = result
        return host, port
    return "127.0.0.1", DEFAULT_PORT


# This processes incoming pickled pipeline objects
class PipelineObjectProtocol(asyncio.Protocol):
    def __init__(self):
        self.transport = None

    # Called when a connection is made.
    # Transport is like a socket but we don't really use it.
    def connection_made(self, transport):
        logging.info("Connection was made ...")
        self.transport = transport

    def connection_lost(self, exc):
        logging.info("Connection was lost ...")

    # This is called when data is incoming.
    # With socket.sendall client side it seems to only call once versus called more than once
    # and having to put the data into a buffer.
    def data_received(self, data):
        try:
            if data.decode() == "poisonpill":
                logging.info("Stopping the loop and shutting down the server ...")
                asyncio.get_event_loop().stop()
                return
        except UnicodeDecodeError:
            jobdata = pickle.loads(data)
            errors = jobdata.validate()
            if errors != "":
                logging.error(
                    "Errors in received JobData object (nothing to do): %s", errors
                )
                return

        logging.info("Submitting job to scheduler ...")
        logging.info(jobdata.toString())
        PbsScheduler.submit(jobdata)


def checkPbsConnection():
    host, port = _get_pbs_address()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    connected = s.connect_ex((host, port))
    s.close()
    logging.info("Checking connection: {} ...".format(connected))
    return connected  # non zero for unconnected


# Used by clients to send to the PbsConsumer
def sendJobToPbs(pipejob, max_retries=3, retry_delay=0.5):
    import time

    host, port = _get_pbs_address()

    # Turn our object into bytes for sending
    serialized = None
    jobData = None
    if pipejob == "poisonpill":
        logging.info("Got poisonpill for sending ...")
        serialized = pipejob.encode()
    else:
        # Store what we need in a new class and pickle
        jobData = JobData(pipejob)

        errors = jobData.validate()
        if errors != "":
            print("Errors in JobData (will not send): %s" % errors)
            return

        serialized = pickle.dumps(jobData)

    logging.info("Sending to server ...")

    # open TCP connection and sendall bytes with retry logic
    for attempt in range(max_retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(serialized)
                return  # Success
        except (ConnectionRefusedError, OSError):
            if attempt < max_retries - 1:
                logging.warning(
                    "Connection refused (attempt %d/%d), retrying in %.1fs ...",
                    attempt + 1, max_retries, retry_delay * (attempt + 1)
                )
                time.sleep(retry_delay * (attempt + 1))
            else:
                logging.error(
                    "Connection refused after %d attempts, attempting auto-restart.",
                    max_retries
                )

    # All retries exhausted — attempt auto-restart (skip for poisonpill)
    if jobData is not None:
        try:
            # Deferred import avoids circular import
            from wpipe.scheduler import pbsconsumer
            pbsconsumer("start")

            # One final send attempt after restart
            host, port = _get_pbs_address()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(serialized)
                logging.info("Job sent successfully after auto-restart.")
                return  # recovery succeeded
        except Exception as restart_exc:
            logging.error("Auto-restart failed: %s", restart_exc)

        # Fall through to save_failed_job
        try:
            failed_job_path = save_failed_job("pbs", jobData)
            logging.error("Failed job saved to %s", failed_job_path)
        except Exception as e:
            logging.error("Failed to save job: %s", str(e))


def periodicLog():
    logging.info("PbsConsumer is still running ...")
    asyncio.get_event_loop().call_later(60 * 30, lambda: periodicLog())


if __name__ == "__main__":
    from wpipe.scheduler.PbsConsumer import DEFAULT_PORT
    from wpipe.scheduler.Utils import write_address_file, remove_address_file

    # Setup the logging
    logging.basicConfig(
        filename="PbsConsumerLog-{}.log".format(datetime.today().strftime("%m-%d-%Y")),
        level=logging.DEBUG,
        filemode="a",
        format="[%(asctime)s][%(levelname)s][%(name)s]: %(message)s",
    )

    # capture stdout into log file
    stdout_logger = logging.getLogger("STDOUT")
    sl = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = sl

    # capture stderr into log file
    stderr_logger = logging.getLogger("STDERR")
    sl = StreamToLogger(stderr_logger, logging.ERROR)
    sys.stderr = sl

    # Setup signal handlers to log unexpected terminations
    setup_signal_handlers("PbsConsumer")

    # Setup loop
    logging.info("Setting up asyncio loop ...")
    loop = asyncio.get_event_loop()

    logging.info("Creating PbsConsumer server on 0.0.0.0:{} ...".format(DEFAULT_PORT))
    coroutine = loop.create_server(
        lambda: PipelineObjectProtocol(), "0.0.0.0", DEFAULT_PORT
    )
    server = loop.run_until_complete(coroutine)

    # Write address file after successful bind — no race window
    actual_hostname = socket.gethostname()
    actual_port = server.sockets[0].getsockname()[1]
    write_address_file("pbs", actual_hostname, actual_port, os.getpid())

    try:
        logging.info("Running loop forever ...")
        if SESSION is not None:
            SESSION.close()
        loop.call_later(60 * 30, lambda: periodicLog())
        loop.run_forever()
    finally:
        # Remove address file first so clients stop trying to connect
        remove_address_file("pbs")

        # Shutdown server
        logging.info("Closing server ...")
        server.close()
        loop.run_until_complete(server.wait_closed())

        # Run existing tasks
        pending = asyncio.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending))
