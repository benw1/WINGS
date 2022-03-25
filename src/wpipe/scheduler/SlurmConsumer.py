#!/usr/bin/env python
"""
Contains the SlurmConsumer utilities including the scheduler.checkSlurmConnection
and scheduler.sendJobToSlurm function definitions

Please note that this module is private. These functions are available in the
main ``wpipe.scheduler`` namespace - use that instead.
"""
import asyncio
import pickle
import socket
import logging
import sys
from datetime import datetime
from .StreamToLogger import StreamToLogger
from .JobData import JobData
from .SlurmScheduler import SlurmScheduler
from wpipe.sqlintf import SESSION

__all__ = ['BASE_PORT', 'DEFAULT_PORT', 'checkSlurmConnection', 'sendJobToSlurm']

# TODO: Make this not hardcoded
HOST_MACHINE = '10.150.27.94'
BASE_PORT = DEFAULT_PORT = 5000


# HOST_MACHINE = '127.0.0.1' # For debugging

# This processes incoming pickled pipeline objects
class PipelineObjectProtocol(asyncio.Protocol):

    def __init__(self):
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
        SlurmScheduler.submit(jobdata)


def checkSlurmConnection():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connected = s.connect_ex((HOST_MACHINE, DEFAULT_PORT))
    s.close()
    logging.info("Checking connection: {} ...".format(connected))
    return connected  # non zero for unconnected


# Used by clients to send to the SlurmConsumer
def sendJobToSlurm(pipejob):
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


def periodicLog():
    logging.info("SlurmConsumer is still running ...")
    asyncio.get_event_loop().call_later(60 * 30, lambda: periodicLog())


if __name__ == "__main__":
    from wpipe.scheduler.SlurmConsumer import DEFAULT_PORT
    # Setup the logging
    logging.basicConfig(filename='SlurmConsumerLog-{}.log'.format(datetime.today().strftime('%m-%d-%Y-%H-%M-%S')),
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

    logging.info('Creating SlurmConsumer server on {}:{} ...'.format(HOST_MACHINE, DEFAULT_PORT))
    coroutine = loop.create_server(lambda: PipelineObjectProtocol(), HOST_MACHINE, DEFAULT_PORT)
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
        loop.call_later(172800, lambda: sendJobToSlurm("poisonpill"))  # This kills the server after some time
        loop.call_later(60 * 30, lambda: periodicLog())
        loop.run_forever()
    finally:
        # Shutdown server
        logging.info('Closing server ...')
        server.close()
        loop.run_until_complete(server.wait_closed())

        # Run existing tasks
        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending))
