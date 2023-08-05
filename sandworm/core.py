import logging
import sys

from . import _graph
from . import target

_logger = logging.getLogger("sandworm.core")


def init_logging(*, fmt: str = "[%(levelname)s] %(message)s", verbose: bool = False) -> None:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt=fmt))

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.addHandler(handler)


def _display_cycle(cycle: list[target.Target]) -> None:
    _logger.error("Dependency cycle detected:")
    base = cycle[0].env.basedir
    for t in cycle:
        _logger.error(f"\t{t.fullname()} from {t.env.basedir.relative_to(base)}")
    _logger.error(f"\t{cycle[0].fullname()} from .")


def root_build(env: target.Environment, main: target.Target) -> bool:
    if (cycle := _graph.Graph(main).find_cycle()) is not None:
        _display_cycle(cycle)
        return False

    if ret := _build_sequence(env, _linearize(main)):
        _logger.info("Build successful")
    return ret


def make_clean(env: target.Environment) -> bool:
    for t in target._clean_targets:
        if (cycle := _graph.Graph(t).find_cycle()) is not None:
            _display_cycle(cycle)
            return False

    sequence: list[target.Target] = []
    for t in reversed(target._clean_targets):
        sequence += _linearize(t)
    return _build_sequence(env, sequence)


def _build_sequence(env: target.Environment, sequence: list[target.Target]) -> bool:
    for targ in sequence:
        if targ.built:
            continue

        _logger.debug(f"Building {targ.fullname()}")
        if not targ.build():
            _logger.error(f"Build failed for {targ.fullname()}")
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
