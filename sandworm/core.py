import logging
import sys

from . import _graph
from . import target

logger = logging.getLogger("sandworm.core")
clean_targets: list[target.Target] = []


def init_logging(formatter: logging.Formatter | None = None) -> None:
    if formatter is None:
        formatter = logging.Formatter(fmt="[%(levelname)s] %(message)s")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)


def _display_cycle(cycle: list[target.Target]) -> None:
    logger.error("Dependency cycle detected:")
    base = cycle[0].env.basedir
    for t in cycle:
        logger.error(f"\t{t.fullname()} from {t.env.basedir.relative_to(base)}")
    logger.error(f"\t{cycle[0].fullname()} from .")


def root_build(env: target.Environment, main: target.Target) -> bool:
    if (cycle := _graph.Graph(main).find_cycle()) is not None:
        _display_cycle(cycle)
        return False

    if ret := _build_sequence(env, _linearize(main)):
        logger.info("Build successful")
    return ret


def add_clean_target(targ: target.Target) -> None:
    clean_targets.append(targ)


def make_clean(env: target.Environment) -> bool:
    for t in clean_targets:
        if (cycle := _graph.Graph(t).find_cycle()) is not None:
            _display_cycle(cycle)
            return False

    sequence: list[target.Target] = []
    for t in reversed(clean_targets):
        sequence += _linearize(t)
    return _build_sequence(env, sequence)


def _build_sequence(env: target.Environment, sequence: list[target.Target]) -> bool:
    for targ in sequence:
        if targ.wait(0.0):
            continue

        logger.debug(f"Building {type(targ).__name__}: {targ.fullname()}")
        if not targ.build():
            logger.error(f"Build failed for {type(targ).__name__}: {targ.fullname()}")
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
