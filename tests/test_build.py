import enum
import os
import pathlib
import time

import pytest

import sandworm


class MaxWorkers(enum.IntEnum):
    SERIAL = 1
    PARALLEL = 4


parametrize_workers = pytest.mark.parametrize("max_workers", MaxWorkers)


def check_builder(targ: sandworm.FileTarget) -> bool:
    with open(targ.name, "w") as f:
        f.write("check\n")
    return True


@parametrize_workers
def test_no_targets(env: sandworm.Environment, max_workers: int) -> None:
    assert sandworm.root_build(env.main_target, max_workers=max_workers)


@parametrize_workers
def test_single_target(env: sandworm.Environment, max_workers: int) -> None:
    env.add_target(sandworm.FileTarget("foo.txt", builder=check_builder), main=True)

    assert env.main_target is not None
    assert sandworm.root_build(env.main_target, max_workers=max_workers)
    path = pathlib.Path("foo.txt")
    assert path.is_file()
    with path.open() as f:
        assert f.read() == "check\n"


def false_builder(targ: sandworm.Target) -> bool:
    return False


@parametrize_workers
def test_fail_build(env: sandworm.Environment, max_workers: int) -> None:
    foo_target = sandworm.FileTarget("foo.txt", builder=false_builder)
    env.add_target(foo_target)

    assert not sandworm.root_build(foo_target, max_workers=max_workers)


@parametrize_workers
def test_target_out_of_date(env: sandworm.Environment, max_workers: int) -> None:
    bar_target = sandworm.FileTarget("bar.txt")
    foo_target = sandworm.FileTarget("foo.txt", dependencies=[bar_target], builder=check_builder)
    env.add_target(bar_target)
    env.add_target(foo_target)

    for name in ("foo", "bar"):
        pathlib.Path(f"{name}.txt").touch()

    later = int(time.time()) + 5
    os.utime("bar.txt", (later, later))

    assert sandworm.root_build(foo_target, max_workers=max_workers)
    path = pathlib.Path("foo.txt")
    assert path.is_file()
    with path.open() as f:
        assert f.read() == "check\n"


@parametrize_workers
def test_target_not_out_of_date(env: sandworm.Environment, max_workers: int) -> None:
    bar_target = sandworm.FileTarget("bar.txt")
    foo_target = sandworm.FileTarget("foo.txt", dependencies=[bar_target], builder=check_builder)
    env.add_target(bar_target)
    env.add_target(foo_target)

    for name in ("bar", "foo"):
        pathlib.Path(f"{name}.txt").touch()

    assert sandworm.root_build(foo_target, max_workers=max_workers)
    path = pathlib.Path("foo.txt")
    assert path.is_file()
    with path.open() as f:
        assert f.read() == ""


def append_builder(targ: sandworm.Target) -> bool:
    with open("foo.txt", "a") as f:
        f.write(f"{targ.name}\n")
    return True


def test_clean(env: sandworm.Environment) -> None:
    env.add_target(sandworm.FileTarget("foo.txt", builder=check_builder), clean=True)

    assert sandworm.make_clean(env)
    path = pathlib.Path("foo.txt")
    assert path.is_file()
    with path.open() as f:
        assert f.read() == "check\n"


def test_clean_targets_in_reverse_order(env: sandworm.Environment) -> None:
    for name in ("foo", "bar"):
        env.add_target(sandworm.Target(name, builder=append_builder), clean=True)

    assert sandworm.make_clean(env)
    path = pathlib.Path("foo.txt")
    assert path.is_file()
    with path.open() as f:
        assert f.read() == "bar\nfoo\n"


def true_builder(targ: sandworm.Target) -> bool:
    return True


def test_fail_cyclic_dependency(env: sandworm.Environment) -> None:
    foo_target = sandworm.Target("foo", builder=true_builder)
    bar_target = sandworm.Target("bar", builder=true_builder, dependencies=[foo_target])
    env.add_target(sandworm.Target("foo", builder=true_builder, dependencies=[bar_target]), main=True)

    assert not sandworm.root_build(env.main_target)


@parametrize_workers
def test_fail_no_rule_to_build(env: sandworm.Environment, max_workers: int) -> None:
    env.add_target(sandworm.Target("foo"), main=True)

    assert not sandworm.root_build(env.main_target, max_workers=max_workers)


@parametrize_workers
def test_no_rule_but_dependencies(env: sandworm.Environment, max_workers: int) -> None:
    bar_target = sandworm.FileTarget("bar.txt", builder=check_builder)
    env.add_target(sandworm.Target("foo", dependencies=[bar_target]), main=True)

    assert sandworm.root_build(env.main_target, max_workers=max_workers)
