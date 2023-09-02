from __future__ import annotations
import concurrent.futures
import dataclasses
import logging
import logging.handlers
import multiprocessing
import multiprocessing.connection
import multiprocessing.queues
import threading
import typing

from . import target

Connection = multiprocessing.connection.Connection
ChildWaiter = Connection | set[Connection] | None
JobPreContext = tuple[ChildWaiter, Connection | None, Connection | None]

logger = logging.getLogger("sandworm.parallel")


@dataclasses.dataclass(slots=True, repr=False, eq=False)
class Job:
    targ: target.Target
    waiter: ChildWaiter
    read_end: Connection
    write_end: Connection


@dataclasses.dataclass(slots=True, repr=False, eq=False)
class ReducedJob:
    targ: target.Target
    read_end: Connection
    write_end: Connection

    @staticmethod
    def from_job(job: Job) -> ReducedJob:
        return ReducedJob(targ=job.targ, read_end=job.read_end, write_end=job.write_end)


def init_job_process(log_queue: multiprocessing.queues.Queue) -> None:
    logging.getLogger().handlers = [logging.handlers.QueueHandler(log_queue)]


def send_job_status(
    fileno_connection: Connection, job: Job | ReducedJob, status: bool, *, fileno: int | None = None
) -> None:
    if fileno is None:
        fileno = job.read_end.fileno()
    job.write_end.send(status)
    job.write_end.close()
    fileno_connection.send(fileno)


def run_job(fileno_connection: Connection, job: ReducedJob, fileno: int) -> None:
    try:
        ret = job.targ.build()
    except Exception:
        logger.exception(f"Job for {job.targ.fullname()} crashed:")
        ret = False
    send_job_status(fileno_connection, job, ret, fileno=fileno)


class JobPool(concurrent.futures.ProcessPoolExecutor):
    def __init__(self, max_workers: int | None, jobs: list[Job]) -> None:
        self._log_queue: multiprocessing.queues.Queue = multiprocessing.Queue()
        super().__init__(max_workers=max_workers, initializer=init_job_process, initargs=(self._log_queue,))
        self._fileno_conn_read, self._fileno_conn_write = multiprocessing.Pipe()
        self._jobs = jobs
        self._pending_connections: dict[int, Connection] = {}
        self._any_failures = False

        for job in jobs:
            self._add_pending_connection(job)

        self._log_thread = threading.Thread(target=self._thread_func)

    def _add_pending_connection(self, job: Job) -> None:
        self._pending_connections[job.read_end.fileno()] = job.read_end

    def _thread_func(self) -> None:
        while (record := self._log_queue.get()) is not None:
            assert isinstance(record, logging.LogRecord)
            logger.handle(record)

    def _handle_job(self, job: Job) -> None:
        if job.targ.builder is None:
            send_job_status(self._fileno_conn_write, job, job.targ.exists)
        else:
            fileno = job.read_end.fileno()
            logger.debug(f"Starting job for target {job.targ.fullname()}")
            self.submit(run_job, self._fileno_conn_write, ReducedJob.from_job(job), fileno)

    def _handle_job_status(self, job: Job, dep_success: bool) -> None:
        if dep_success:
            self._handle_job(job)
        else:
            send_job_status(self._fileno_conn_write, job, False)

    def _handle_ready_connection(self, conn: Connection) -> None:
        success: bool = conn.recv()
        if not success:
            self._any_failures = True
        conn.close()
        indices_to_remove: set[int] = set()
        for k, job in enumerate(self._jobs):
            assert job.waiter is not None
            job_finished = False
            if isinstance(job.waiter, Connection):
                if job.waiter is conn:
                    job_finished = True
            elif conn in job.waiter:
                job.waiter.remove(conn)
                if not job.waiter:
                    job_finished = True

            if job_finished:
                self._handle_job_status(job, success)
                indices_to_remove.add(k)

        if indices_to_remove:
            self._jobs = [job for k, job in enumerate(self._jobs) if k not in indices_to_remove]

    def run(self, leaves: list[Job]) -> bool:
        logger.debug("Starting job pool")

        for leaf in leaves:
            self._add_pending_connection(leaf)
            self._handle_job(leaf)

        while self._pending_connections:
            fileno: int = self._fileno_conn_read.recv()
            if (conn := self._pending_connections.pop(fileno, None)) is not None:
                self._handle_ready_connection(conn)

        logger.debug("Job pool finished")

        return not self._any_failures

    def __enter__(self) -> JobPool:
        self._log_thread.start()
        super().__enter__()
        return self

    def __exit__(self, *args: typing.Any) -> typing.Any:
        ret = super().__exit__(*args)

        self._log_queue.put(None)
        self._log_thread.join()

        return ret


def populate_job_pre_map(
    job_pre_map: dict[target.Target, JobPreContext], targ: target.Target
) -> JobPreContext:
    if (ctx := job_pre_map.get(targ)) is not None:
        return ctx

    child_waiter_set: set[Connection] = set()
    for dep in targ.dependencies:
        dep_ctx = populate_job_pre_map(job_pre_map, dep)
        if (second_slot := dep_ctx[1]) is None:
            if isinstance(first_slot := dep_ctx[0], Connection):
                child_waiter_set.add(first_slot)
            elif first_slot is not None:
                child_waiter_set |= first_slot
        else:
            child_waiter_set.add(second_slot)

    child_waiter: ChildWaiter
    match len(child_waiter_set):
        case 0:
            child_waiter = None
        case 1:
            child_waiter = next(iter(child_waiter_set))
        case _:
            child_waiter = child_waiter_set

    read_end: Connection | None
    write_end: Connection | None
    if targ.builder is None and targ.dependencies:
        read_end = write_end = None
    else:
        read_end, write_end = multiprocessing.Pipe()

    ctx = (child_waiter, read_end, write_end)
    job_pre_map[targ] = ctx
    return ctx


def parallel_root_build(main: target.Target, max_workers: int | None) -> bool:
    logger.debug("Determining target dependencies")

    job_pre_map: dict[target.Target, JobPreContext] = {}
    populate_job_pre_map(job_pre_map, main)

    jobs: list[Job] = []
    leaves: list[Job] = []
    for targ, (waiter, read_end, write_end) in job_pre_map.items():
        if write_end is None:
            continue
        assert read_end is not None
        job = Job(targ=targ, waiter=waiter, read_end=read_end, write_end=write_end)
        if waiter is None:
            leaves.append(job)
        else:
            jobs.append(job)
    del job_pre_map

    with JobPool(max_workers, jobs) as pool:
        del jobs
        return pool.run(leaves)
