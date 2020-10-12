import asyncio
import pickle
import socket
from .JobData import JobData
from .PbsScheduler import PbsScheduler
#from wpipe.sqlintf.core import session

# This processes incoming pickled pipeline objects
class PipelineObjectProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport = None

    # Called when a connection is made.
    # Transport is like a socket but we don't really use it.
    def connection_made(self, transport):
        print("Connection was made")
        self.transport = transport

    # This is called when data is incoming.
    # With socket.sendall client side it seems to only call once versus called more than once
    # and having to put the data into a buffer.
    def data_received(self, data):
        try:
            if data.decode() == "poisonpill":
                print("shutting down the server")
                asyncio.get_event_loop().stop()
                return
        except UnicodeDecodeError:
            jobdata = pickle.loads(data)
            errors = jobdata.validate()
            if errors != "":
                print("Errors in JobData: %s" % errors)
                return

        PbsScheduler.submit(jobdata)


def checkPbsConnection():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connected = s.connect_ex(('127.0.0.1', 5000))
    s.close()
    return connected # non zero for unconnected

# Used by clients to send to the PbsConsumer
def sendJobToPbs(pipejob):
    import re
    #hostMachine = re.search('(?<=@).+(?=:)', str(session.get_bind().url))
    hostMachine = '10.150.27.94'

    if hostMachine is None:
        hostMachine = 'localhost'
    #else:
    #    hostMachine = hostMachine.group()

    # Turn our object into bytes for sending
    serialized = None
    if pipejob == "poisonpill":
        serialized = pipejob.encode()
    else:
        # Store what we need in a new class and pickle
        jobData = JobData(pipejob)
        errors = jobData.validate()

        if errors != "":
            print("Errors in JobData: %s" % errors)
            return

        serialized = pickle.dumps(jobData)

    # open TCP connection and sendall bytes
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((hostMachine, 5000))
        s.sendall(serialized)
        s.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    coroutine = loop.create_server(lambda: PipelineObjectProtocol(), '127.0.0.1', 5000)
    server = loop.run_until_complete(coroutine)

    try:
        # TODO: Make this more sophisticated
        # Set to turn off after two days.
        loop.call_later(172800, lambda: sendJobToPbs("poisonpill"))  # This kills the server after some time
        loop.run_forever()
    finally:
        # Shutdown server
        server.close()
        loop.run_until_complete(server.wait_closed())

        # Run existing tasks
        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending))
