import importlib
import logging
import pathlib
import sys

from . import _graph
from . import errors
from . import target

logger = logging.getLogger("sandworm.core")
clean_targets: list[target.Target] = []


def init_logging(handler: logging.Handler | None = None) -> None:
    if handler is None:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter(fmt="%(name)s [%(levelname)s]: %(message)s"))

    logger = logging.getLogger()
    logger.addHandler(handler)


def display_cycle(cycle: list[target.Target]) -> None:
    print("Dependency cycle detected:", file=sys.stderr)
    base = cycle[0].basedir
    for t in cycle:
        print(f"\t{t.fullname()} from {t.origin.relative_to(base)}", file=sys.stderr)
    print(f"\t{cycle[0].fullname()} from {cycle[0].origin.relative_to(base)}", file=sys.stderr)


def root_build(env: target.Environment, main: target.Target) -> bool:
    if (cycle := _graph.Graph(main).find_cycle()) is not None:
        display_cycle(cycle)
        return False

    if ret := build_sequence(env, linearize(main)):
        logger.info("Build successful")
    return ret


def add_clean_target(targ: target.Target) -> None:
    clean_targets.append(targ)


def make_clean(env: target.Environment) -> bool:
    for t in clean_targets:
        if (cycle := _graph.Graph(t).find_cycle()) is not None:
            display_cycle(cycle)
            return False

    sequence: list[target.Target] = []
    for t in reversed(clean_targets):
        sequence += linearize(t)
    return build_sequence(env, sequence)


def build_sequence(env: target.Environment, sequence: list[target.Target]) -> bool:
    for targ in sequence:
        if targ.built():
            continue

        logger.debug(f"Building {type(targ).__name__}: {targ.fullname()}")
        if not targ.builder(env, targ):
            logger.error(f"Build failed for {type(targ).__name__}: {targ.fullname()}")
            return False
        targ.set_built()

    return True


def linearize_recurse(targ: target.Target, records: dict[target.Target, int], count: int) -> int:
    for dep in targ.dependencies:
        count += linearize_recurse(dep, records)

    if targ not in records and targ.out_of_date():
        records[targ] = count
        count += 1

    return count


def linearize(main: target.Target) -> list[target.Target]:
    records: dict[target.Target, int] = {}
    linearize_recurse(main, records)

    return [targ for targ, _ in sorted(records.items(), key=lambda x: x[1])]
