from __future__ import annotations
import collections.abc
import concurrent.futures
import dataclasses
import itertools
import logging
import logging.handlers
import multiprocessing
import multiprocessing.queues
import threading
import typing

from . import target

JobDeps = int | set[int] | None

logger = logging.getLogger("sandworm.parallel")

# Populated by init_job.
job_queue: multiprocessing.queues.Queue


@dataclasses.dataclass(slots=True, repr=False, eq=False)
class JobPreContext:
    token: int | None
    deps: JobDeps


@dataclasses.dataclass(slots=True, repr=False, eq=False)
class Job:
    targ: target.Target
    token: int
    deps: JobDeps


@dataclasses.dataclass(slots=True, repr=False, eq=False)
class JobResult:
    token: int
    success: bool


def init_job(j_queue: multiprocessing.queues.Queue, log_queue: multiprocessing.queues.Queue) -> None:
    global job_queue
    job_queue = j_queue

    logging.getLogger().handlers = [logging.handlers.QueueHandler(log_queue)]


def send_job_result(q: multiprocessing.queues.Queue, job: Job, success: bool) -> None:
    q.put(JobResult(token=job.token, success=success))


def run_job(job: Job) -> None:
    try:
        success = job.targ.build()
    except Exception:
        success = False
        raise
    finally:
        send_job_result(job_queue, job, success)


class JobPool(concurrent.futures.ProcessPoolExecutor):
    def __init__(self, max_workers: int | None, jobs: list[Job]) -> None:
        self._job_queue: multiprocessing.queues.Queue = multiprocessing.Queue()
        self._log_queue: multiprocessing.queues.Queue = multiprocessing.Queue()
        super().__init__(
            max_workers=max_workers,
            initializer=init_job,
            initargs=(
                self._job_queue,
                self._log_queue,
            ),
        )
        self._jobs = jobs
        self._pending_jobs: dict[int, Job] = {}
        self._running_futures: dict[int, concurrent.futures.Future] = {}
        self._any_failures = False

        for job in jobs:
            self._pending_jobs[job.token] = job

        self._log_thread = threading.Thread(target=self._thread_func)

    def _thread_func(self) -> None:
        while (record := self._log_queue.get()) is not None:
            assert isinstance(record, logging.LogRecord)
            logger.handle(record)

    def _handle_job(self, job: Job) -> None:
        if job.targ.builder is None:
            send_job_result(self._job_queue, job, job.targ.exists)
        else:
            logger.debug(f"Starting job for target {job.targ.fullname()}")
            self._running_futures[job.token] = self.submit(run_job, job)

    def _handle_job_status(self, job: Job, dep_success: bool) -> None:
        if dep_success:
            self._handle_job(job)
        else:
            send_job_result(self._job_queue, job, False)

    def _handle_finished_job(self, result: JobResult) -> None:
        if not result.success:
            self._any_failures = True
        indices_to_remove: set[int] = set()
        for k, job in enumerate(self._jobs):
            assert job.deps is not None
            if isinstance(job.deps, int):
                job_finished = job.deps == result.token
            elif result.token in job.deps:
                job.deps.remove(result.token)
                job_finished = not bool(job.deps)

            if job_finished:
                self._handle_job_status(job, result.success)
                indices_to_remove.add(k)

        if indices_to_remove:
            self._jobs = [job for k, job in enumerate(self._jobs) if k not in indices_to_remove]

    def run(self, leaves: list[Job]) -> bool:
        logger.debug("Starting job pool")

        for leaf in leaves:
            self._pending_jobs[leaf.token] = leaf
            self._handle_job(leaf)

        while self._pending_jobs:
            result: JobResult = self._job_queue.get()
            job = self._pending_jobs.pop(result.token)

            if (future := self._running_futures.pop(result.token, None)) is not None:
                try:
                    future.result()
                except Exception:
                    logger.exception(f"Exception caught building {job.targ.fullname()}:")

            self._handle_finished_job(result)

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
    job_pre_map: dict[target.Target, JobPreContext],
    counter: collections.abc.Iterator[int],
    targ: target.Target,
) -> JobPreContext:
    if (ctx := job_pre_map.get(targ)) is not None:
        return ctx

    token_set: set[int] = set()
    for dep in targ.dependencies:
        if not dep.out_of_date:
            continue

        dep_ctx = populate_job_pre_map(job_pre_map, counter, dep)
        if dep_ctx.token is None:
            if isinstance(dep_ctx.deps, int):
                token_set.add(dep_ctx.deps)
            elif dep_ctx.deps is not None:
                token_set |= dep_ctx.deps
        else:
            token_set.add(dep_ctx.token)

    job_deps: JobDeps
    match len(token_set):
        case 0:
            job_deps = None
        case 1:
            job_deps = next(iter(token_set))
        case _:
            job_deps = token_set

    token: int | None
    if targ.builder is None and targ.dependencies:
        token = None
    else:
        token = next(counter)
    ctx = JobPreContext(token=token, deps=job_deps)
    job_pre_map[targ] = ctx
    return ctx


def parallel_root_build(main: target.Target, max_workers: int | None) -> bool:
    logger.debug("Determining target dependencies")

    job_pre_map: dict[target.Target, JobPreContext] = {}
    populate_job_pre_map(job_pre_map, itertools.count(), main)

    jobs: list[Job] = []
    leaves: list[Job] = []
    for targ, ctx in job_pre_map.items():
        if ctx.token is None:
            continue
        job = Job(targ=targ, token=ctx.token, deps=ctx.deps)
        if job.deps is None:
            leaves.append(job)
        else:
            jobs.append(job)
    del job_pre_map

    with JobPool(max_workers, jobs) as pool:
        del jobs
        return pool.run(leaves)
