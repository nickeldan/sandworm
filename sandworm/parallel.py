import concurrent.futures
import dataclasses
import logging
import multiprocessing
import multiprocessing.connection
import multiprocessing.queues
import typing

from . import target

_Connection = multiprocessing.connection.Connection
_ChildWaiter = _Connection | set[_Connection] | None
_JobPreContext = tuple[_ChildWaiter, _Connection | None, _Connection | None]

_logger = logging.getLogger("sandworm.parallel")


@dataclasses.dataclass(slots=True, repr=False, eq=False)
class _Job:
    targ: target.Target
    waiter: _ChildWaiter
    read_end: _Connection
    write_end: _Connection


def _send_job_status(fileno_queue: multiprocessing.queues.Queue, job: _Job, status: bool) -> None:
    fileno = job.write_end.fileno()
    job.write_end.send(status)
    job.write_end.close()
    fileno_queue.put(fileno)


def _run_job(fileno_queue: multiprocessing.queues.Queue, job: _Job) -> None:
    job.read_end.close()
    _send_job_status(fileno_queue, job, job.targ.build())


class _JobPool(concurrent.futures.ProcessPoolExecutor):
    def __init__(self, max_workers: int | None, jobs: list[_Job]) -> None:
        super().__init__(max_workers=max_workers)
        self._jobs = jobs
        self._pending_connections: dict[int, _Connection] = {}
        self._fileno_queue: multiprocessing.queues.Queue = multiprocessing.Queue()
        self._any_failures = False

        for job in jobs:
            if isinstance(job.waiter, _Connection):
                self._pending_connections[job.waiter.fileno()] = job.waiter
            else:
                assert job.waiter is not None
                for conn in job.waiter:
                    self._pending_connections[conn.fileno()] = conn

    def shutdown(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        kwargs["cancel_futures"] = True
        super().shutdown(*args, **kwargs)

    def _handle_job(self, job: _Job) -> None:
        if job.targ.builder is None:
            _send_job_status(self._fileno_queue, job, job.targ.exists)
        else:
            self.submit(_run_job, self._fileno_queue, job)

    def _handle_job_status(self, job: _Job, dep_success: bool) -> None:
        if dep_success:
            self._handle_job(job)
        else:
            _send_job_status(self._fileno_queue, job, False)
            self._any_failures = True

    def _handle_ready_connection(self, conn: _Connection) -> None:
        success: bool = conn.recv()
        conn.close()
        indices_to_remove: set[int] = set()
        for k, job in enumerate(self._jobs):
            assert job.waiter is not None
            if isinstance(job.waiter, _Connection):
                if job.waiter is conn:
                    self._handle_job_status(job, success)
                    indices_to_remove.add(k)
            elif conn in job.waiter:
                job.waiter.remove(conn)
                if not job.waiter:
                    self._handle_job_status(job, success)
                indices_to_remove.add(k)

        if indices_to_remove:
            self._jobs = [job for k, job in enumerate(self._jobs) if k not in indices_to_remove]

    def run(self, leaves: list[_Job]) -> bool:
        for leaf in leaves:
            self._handle_job(leaf)

        while self._jobs:
            fileno: int = self._fileno_queue.get()
            conn = self._pending_connections.pop(fileno)
            self._handle_ready_connection(conn)

        return self._any_failures


def _populate_job_pre_map(
    job_pre_map: dict[target.Target, _JobPreContext], targ: target.Target
) -> _JobPreContext:
    if (ctx := job_pre_map.get(targ)) is not None:
        return ctx

    child_waiter_set: set[_Connection] = set()
    for dep in targ.dependencies:
        dep_ctx = _populate_job_pre_map(job_pre_map, dep)
        if (second_slot := dep_ctx[1]) is None:
            if isinstance(first_slot := dep_ctx[0], _Connection):
                child_waiter_set.add(first_slot)
            elif first_slot is not None:
                child_waiter_set |= first_slot
        else:
            child_waiter_set.add(second_slot)

    child_waiter: _ChildWaiter
    match len(child_waiter_set):
        case 0:
            child_waiter = None
        case 1:
            child_waiter = next(iter(child_waiter_set))
        case _:
            child_waiter = child_waiter_set

    read_end: _Connection | None
    write_end: _Connection | None
    if targ.builder is None and targ.dependencies:
        read_end = write_end = None
    else:
        read_end, write_end = multiprocessing.Pipe()

    ctx = (child_waiter, read_end, write_end)
    job_pre_map[targ] = ctx
    return ctx


def root_parallel_build(main: target.Target, max_workers: int | None) -> bool:
    job_pre_map: dict[target.Target, _JobPreContext] = {}
    _populate_job_pre_map(job_pre_map, main)

    jobs: list[_Job] = []
    leaves: list[_Job] = []
    for targ, (waiter, read_end, write_end) in job_pre_map.items():
        if write_end is None:
            continue
        assert read_end is not None
        job = _Job(targ=targ, waiter=waiter, read_end=read_end, write_end=write_end)
        if waiter is None:
            leaves.append(job)
        else:
            jobs.append(job)
    del job_pre_map

    with _JobPool(max_workers, jobs) as pool:
        del jobs
        ret = pool.run(leaves)

    if ret:
        _logger.info("Build successful")
    return ret
