import logging
import os
import sys
import typing

from . import _graph
from . import _parallel
from . import target

_logger = logging.getLogger("sandworm.core")


class _ColorFormatter(logging.Formatter):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().__init__(*args, **kwargs)
        self.is_tty = os.isatty(sys.stdout.fileno())

    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        if self.is_tty and record.levelno >= logging.ERROR:
            msg = "\x1b[31m" + msg + "\x1b[0m"
        return msg


def init_logging(
    *, fmt: str = "%(message)s", verbose: bool = False, stream: typing.TextIO = sys.stdout
) -> None:
    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(_ColorFormatter(fmt=fmt))

    logger = logging.getLogger("sandworm")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.addHandler(handler)


def _display_cycle(cycle: list[target.Target]) -> None:
    _logger.error("Dependency cycle detected:")
    base = cycle[0].env.basedir
    for t in cycle:
        _logger.error(f"\t{t.fullname()} from {t.env.basedir.relative_to(base)}")
    _logger.error(f"\t{cycle[0].fullname()} from .")


def root_build(main: target.Target, max_workers: int | None = 1) -> bool:
    if (cycle := _graph.Graph(main).find_cycle()) is not None:
        _display_cycle(cycle)
        return False

    if not main.out_of_date:
        return True

    if max_workers == 1:
        ret = _build_sequence(_linearize(main))
    else:
        ret = _parallel.parallel_root_build(main, max_workers)

    if ret:
        _logger.info("Build successful")
    return ret


def make_clean(env: target.Environment) -> bool:
    for t in env.clean_targets:
        if (cycle := _graph.Graph(t).find_cycle()) is not None:
            _display_cycle(cycle)
            return False

    sequence: list[target.Target] = []
    for t in reversed(env.clean_targets):
        sequence += _linearize(t)
    return _build_sequence(sequence)


def _build_sequence(sequence: list[target.Target]) -> bool:
    for targ in sequence:
        if targ.built:
            continue

        if not targ.build():
            return False

    return True


def _linearize_recurse(targ: target.Target, records: dict[target.Target, int], count: int) -> int:
    for dep in targ.dependencies:
        count += _linearize_recurse(dep, records, count)

    if targ not in records and targ.out_of_date:
        records[targ] = count
        count += 1

    return count


def _linearize(main: target.Target) -> list[target.Target]:
    records: dict[target.Target, int] = {}
    _linearize_recurse(main, records, 0)

    return [targ for targ, _ in sorted(records.items(), key=lambda x: x[1])]
