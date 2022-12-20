#!/usr/bin/env python
"""
Contains the BaseConsumer utilities including the scheduler.checkConsumerConnection
and scheduler.sendJobToConsumer function definitions

Please note that this module is private. These functions are available in the
main ``wpipe.scheduler`` namespace - use that instead.
"""
import asyncio
import pickle
import socket
import logging
import sys
from datetime import datetime
from pathlib import Path

from .StreamToLogger import StreamToLogger
from .JobData import JobData
from .PbsScheduler import PbsScheduler
from .SlurmScheduler import SlurmScheduler
from wpipe.sqlintf import SESSION

__all__ = ['BASE_PORT', 'DEFAULT_PORT', 'checkConsumerConnection', 'sendJobToConsumer', 'consumer_main']

# TODO: Make this not hardcoded
my_file = Path("/usr/lusers/benw1/server.address")
if  my_file.is_file():
    ip1 = my_file.read_text()
    ip = ip1.strip()
    HOST_MACHINE = ip
else:
    HOST_MACHINE = '10.64.57.84'
BASE_PORT = DEFAULT_PORT = 8000


# HOST_MACHINE = '127.0.0.1' # For debugging

SCHEDULER_DICT = {'Pbs': PbsScheduler, 'Slurm': SlurmScheduler}


# This processes incoming pickled pipeline objects
class PipelineObjectProtocol(asyncio.Protocol):

    def __init__(self, kind: str):
        self._scheduler = SCHEDULER_DICT[kind]
        self.transport = None

    # Called when a connection is made.
    # Transport is like a socket but we don't really use it.
    def connection_made(self, transport):
        logging.info('Connection was made ...')
        self.transport = transport

    def connection_lost(self, exc):
        logging.info('Connection was lost ...')

    # This is called when data is incoming.
    # With socket.sendall client side it seems to only call once versus called more than once
    # and having to put the data into a buffer.
    def data_received(self, data):
        try:
            if data.decode() == "poisonpill":
                logging.info('Stopping the loop and shutting down the server ...')
                asyncio.get_event_loop().stop()
                return
        except UnicodeDecodeError:
            jobdata = pickle.loads(data)
            errors = jobdata.validate()
            if errors != "":
                logging.error("Errors in received JobData object (nothing to do): %s" % errors)
                return

        logging.info('Submitting job to scheduler ...')
        logging.info(jobdata.toString())
        self.scheduler.submit(jobdata)
    
    @property
    def scheduler(self):
        return self._scheduler


def checkConsumerConnection():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connected = s.connect_ex((HOST_MACHINE, DEFAULT_PORT))
    s.close()
    logging.info("Checking connection: {} ...".format(connected))
    return connected  # non zero for unconnected


# Used by clients to send to the PbsConsumer
def sendJobToConsumer(pipejob):
    # TODO: How do we parse for the host machine automatically?

    # Turn our object into bytes for sending
    serialized = None
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

    logging.info('Sending to server ...')

    # open TCP connection and sendall bytes
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST_MACHINE, DEFAULT_PORT))
        s.sendall(serialized)
        s.close()


def periodicLog(kind: str):
    logging.info(f"{kind}Consumer is still running ...")
    asyncio.get_event_loop().call_later(60 * 30, lambda: periodicLog(kind=kind))


def consumer_main(kind: str):
    from wpipe.scheduler.BaseConsumer import DEFAULT_PORT
    # Setup the logging
    logging.basicConfig(filename=f"{kind}ConsumerLog-{datetime.today().strftime('%m-%d-%Y-%H-%M-%S')}.log",
                        level=logging.DEBUG, filemode='a',
                        format="[%(asctime)s][%(levelname)s][%(name)s]: %(message)s")

    # capture stdout into log file
    stdout_logger = logging.getLogger('STDOUT')
    sl = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = sl

    # capture stderr into log file
    stderr_logger = logging.getLogger('STDERR')
    sl = StreamToLogger(stderr_logger, logging.ERROR)
    sys.stderr = sl

    # Setup loop
    logging.info("Setting up asyncio loop ...")
    loop = asyncio.get_event_loop()

    logging.info(f"Creating {kind}Consumer server on {HOST_MACHINE}:{DEFAULT_PORT} ...")
    coroutine = loop.create_server(lambda: PipelineObjectProtocol(kind=kind), HOST_MACHINE, DEFAULT_PORT)
    server = loop.run_until_complete(coroutine)

    # log_loop_task = loop.create_task(periodicLog())
    # loop.run_until_complete(log_loop_task)

    try:
        # TODO: Make this more sophisticated
        # Set to turn off after two days.
        logging.info('Running loop forever ...')
        if SESSION is not None:
            SESSION.close()
            # SESSION = None
        loop.call_later(172800, lambda: sendJobToConsumer("poisonpill"))  # This kills the server after some time
        loop.call_later(60 * 30, lambda: periodicLog(kind=kind))
        loop.run_forever()
    finally:
        # Shutdown server
        logging.info('Closing server ...')
        server.close()
        loop.run_until_complete(server.wait_closed())

        # Run existing tasks
        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending))
