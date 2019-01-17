"""
This module contains a simple job queueing server and necessary utilities to go with it.
The main intention is to use it for batch processing of TRAK simulations.
"""

import os
from multiprocessing.connection import Listener, Client
from threading import Thread
from queue import Queue
import logging

ADDRESS = ("localhost", 6000)
LOGFORMAT = "[%(asctime)s][%(name)s][%(levelname)s]:%(message)s"
SERVERLOG = "./server.log"
AUTHKEY = os.environ["QSERVERPASS"].encode()

def start_logger(logfile=None, name=None, log_level=logging.INFO):
    """
    Starts a logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger_stream_handler = logging.StreamHandler()
    logger_stream_handler.formatter = logging.Formatter(LOGFORMAT)
    logger.addHandler(logger_stream_handler)
    if logfile:
        logger_file_handler = logging.FileHandler(logfile, mode="a")
        logger_file_handler.formatter = logging.Formatter(LOGFORMAT)
        logger.addHandler(logger_file_handler)
    return logger

def submit(jobs, address=ADDRESS):
    """
    Submits a list of objects of type QJob to the server under the given address
    """
    with Client(address, authkey=AUTHKEY) as con:
        con.send(jobs)

class ListenerDaemon(Thread):
    """
    A deamon thread running in the background waiting for connection attempts in order to submit
    jobs to the server. Can only handle one connection at a time. Times out if a connections is
    opened but no data received.
    """
    def __init__(self, address, queue, logger=None):
        super().__init__()
        self.daemon = True
        self.address = address
        self.queue = queue
        self.logger = logger or start_logger()

    def run(self):
        with Listener(address=self.address, authkey=AUTHKEY) as listener:
            while True:
                self.logger.info("Listening for incoming connection...")
                with listener.accept() as con:
                    client = listener.last_accepted
                    self.logger.info("Connection accepted from %s.", client)
                    self._receive(con, client)

    def _receive(self, con, client):
        if con.poll(timeout=5):
            jobs = con.recv()
            self.logger.info("Received data from %s.", client)
            misses = 0
            for job in jobs:
                if isinstance(job, QJob):
                    self.queue.put(job)
                else:
                    misses += 1
                    self.logger.error("Received some data that is not of type QJob.")
            self.logger.info("Queued %d new job(s).", len(jobs)-misses)
            if misses > 0:
                self.logger.info("Discarded %d chunk(s) of useless received data.", misses)
        else:
            self.logger.error("Timeout while waiting for data, closing connection to %s.", client)

class QJob:
    """
    The class defining a job object which includes the necessary instructions and parameters for
    the tasks within this job to be performed
    """
    pass

class QServer:
    """
    The main server class which opens the required daemons and handles the user input
    """
    def __init__(self, address=ADDRESS):
        self.address = address
        self.logger = start_logger(logfile=SERVERLOG, name="QS")
        self.queue = Queue()
        self.listener = ListenerDaemon(self.address, self.queue, logger=logging.getLogger("QS.LD"))

    def start(self):
        """Starts the server and the required components"""
        self.logger.info("Starting server.")
        self.logger.info("Starting listener daemon.")
        self.listener.start()
        while True:
            pass


def main():
    """
    main method to be called if this file is run as a script, launches a server ready to receive
    submissions
    """
    qserver = QServer()
    qserver.start()


if __name__ == "__main__":
    main()