"""
This module contains a simple job queueing server and necessary utilities to go with it.
The main intention is to use it for batch processing of TRAK simulations.
"""

import os
import time
from multiprocessing.connection import Listener, Client
from threading import Thread
from queue import Queue
import logging

# General module/script settings
ADDRESS = ("localhost", 6000)
AUTHKEY = os.environ["QSERVERPASS"].encode()
LOGFORMAT = "[%(asctime)s][%(name)s][%(levelname)s]:%(message)s"
SERVERLOG = "./server.log"
LOGLEVEL = logging.debug

def start_logger(logfile=None, name=None, log_level=LOGLEVEL):
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

def submit_qjobs(jobs, address=ADDRESS):
    """
    Submits a list of objects of type QJob to the server under the given address
    """
    if not isinstance(jobs, list):
        jobs = [jobs]
    with Client(address, authkey=AUTHKEY) as con:
        con.send([job.serialise() for job in jobs])

class QListenerDaemon(Thread):
    """
    A deamon thread running in the background waiting for connection attempts in order to submit
    jobs to the server. Can only handle one connection at a time. Times out if a connections is
    opened but no data received.
    """
    def __init__(self, address, jobqueue, logger=None):
        super().__init__()
        self.daemon = True
        self.address = address
        self.jobqueue = jobqueue
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
                try:
                    job = QJob.deserialise(job)
                    self.jobqueue.put(job)
                except ValueError as err:
                    misses += 1
                    self.logger.error("Received some data that is not a valid QJob.")
                    self.logger.debug(err)
                    self.logger.debug(str(job))
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
    def __init__(self, name=""):
        self.name = name
        self._tasks = []
        self._task_counter = 0
        self._active = False
        self._finished = False
        self._crashed = False

    def serialise(self):
        """Very basic serialisation of the object by returning class name and dict content"""
        return (str(self.__class__), self.__dict__.copy())

    @staticmethod
    def deserialise(serialised_qjob):
        """Recreates a job from a serialisation acquired by serialise method"""
        if "QJob" in serialised_qjob[0]:
            job = QJob()
            for key, val in serialised_qjob[1].items():
                job.__setattr__(key, val)
            return job
        else:
            raise ValueError("Not a valid QJob serialisation")

    @property
    def num_tasks(self):
        """Total number of tasks within the job"""
        return len(self._tasks)

    @property
    def current_task(self):
        """Returns the number of the current task (as 1-indexed)"""
        return self._task_counter + 1

    @property
    def active(self):
        """Reports whether the job is currently active"""
        return self._active

    @property
    def finished(self):
        """Reports whether the job has finished"""
        return self._finished

    @property
    def crashed(self):
        """Reports whether the job has crashed"""
        return self._crashed

    def add_task(self, task):
        """Add a task to this job (appended at the end)"""
        self._tasks.append(task)

    def get_next_task(self):
        """Grab a task and mark the job as active"""
        if self._active:
            raise Exception("Requested task while job is active.")
        if self._finished:
            raise Exception("Requested task after job has finished.")
        if self._crashed:
            raise Exception("Requested task after job has crashed.")
        self._active = True
        return self._tasks[self._task_counter]

    def cancel_active_task(self):
        """Cancels the active task"""
        if not self._active:
            raise Exception("Cancelled task while job was not active.")
        if self._finished:
            raise Exception("Cancelled task after job has finished.")
        if self._crashed:
            raise Exception("Cancelled task after job has crashed.")
        self._active = False

    def report_success(self):
        """Mark current job as succesfull and mark the job as inactive"""
        if not self._active:
            raise Exception("Reported success while job was not active.")
        if self._finished:
            raise Exception("Reported success after job has finished.")
        if self._crashed:
            raise Exception("Reported success after job has crashed.")
        self._active = False
        if self._task_counter == self.num_tasks:
            self._finished = True
        else:
            self._task_counter += 1

    def report_crash(self):
        """Report that the current task/job has crashed"""
        if not self._active:
            raise Exception("Reported crash while job was not active.")
        if self._finished:
            raise Exception("Reported crash after job has finished.")
        if self._crashed:
            raise Exception("Reported crash after job has crashed.")
        self._active = False
        self._crashed = True

class QServer:
    """
    The main server class which opens the required daemons and handles the user input
    """
    def __init__(self, address=ADDRESS):
        self.address = address
        self.logger = start_logger(logfile=SERVERLOG, name="QS")
        self.queue = Queue()
        self.listener = QListenerDaemon(self.address, self.queue, logger=logging.getLogger("QS.LD"))

    def start(self):
        """Starts the server and the required components"""
        self.logger.info("Starting server.")
        self.logger.info("Starting listener daemon.")
        self.listener.start()
        while True:
            entry_time = time.time()
            # Limit loop speed to 10Hz
            time.sleep(max(0, .1-(time.time()-entry_time)))

class QManager(Thread):
    """Manager for the individual workers"""
    INSTR_STOP = 0
    # INSTR_ADD_WORKER = 1
    # INSTR_REM_WORKER = 2
    INSTR_SET_WORKERS = 3
    INSTR_KILL_WORKER = 4
    def __init__(self, jobqueue, instructionqueue):
        super().__init__()
        self.jobqueue = jobqueue
        self.instructionqueue = instructionqueue
        self.workereventqueue = Queue()
        self.logger = start_logger(logfile=SERVERLOG, name="QS.QM")
        self.worker_target = os.cpu_count()
        self.logger.info("Set target number of workers to %d.", self.worker_target)
        self.workers = []

    def run(self):
        while True:
            entry_time = time.time()
            if not self.instructionqueue.empty():
                instr = self.instructionqueue.get()
                instr_code = instr[0]
                instr_args = instr[1]
                if instr_code == QManager.INSTR_STOP:
                    self.logger.info("Breaking from QManager.")
                    break
                if instr_code == QManager.INSTR_SET_WORKERS:
                    self.worker_target = instr_args
                    self.logger.info("Set target number of workers to %d.", self.worker_target)
                if instr_code == QManager.INSTR_KILL_WORKER:
                    raise NotImplementedError
            if not self.workereventqueue.empty():
                pass
            # Limit loop speed to 10Hz
            time.sleep(max(0, .1-(time.time()-entry_time)))

    def _get_free_id(self):
        return min(set(range(len(self.target_workers)))-set([w.idx for w in self.workers]))



class QWorker(Thread):
    """Worker thread"""
    EVENT_SUCCESS = 0
    EVENT_FAIL = 1
    def __init__(self, idx, workereventqueue):
        super().__init__()
        self.idx = idx
        self.workereventqueue = workereventqueue
        self.logger = start_logger(logfile=SERVERLOG, name="QS.W"+str(idx))
        self.running = True

def main():
    """
    main method to be called if this file is run as a script, launches a server ready to receive
    submissions
    """
    qserver = QServer()
    qserver.start()


if __name__ == "__main__":
    main()
