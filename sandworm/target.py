from __future__ import annotations
import collections.abc
import functools
import importlib
import logging
import os
import pathlib
import pickle
import typing

_T = typing.TypeVar("_T", bound="Target")
Builder = collections.abc.Callable[[_T], bool]

_logger = logging.getLogger("sandworm.target")

_sentinel = object()


def _dummy_builder(targ: _T) -> bool:
    return True


class Target:
    def __init__(
        self: _T,
        name: str,
        *,
        dependencies: collections.abc.Iterable[Target] = (),
        builder: Builder[_T] | None = None,
    ) -> None:
        self._name = name
        self.dependencies = list(dependencies)
        self._builder = builder
        self._env: Environment | None = None
        self._built = False

        if builder is not None:
            try:
                pickle.dumps(builder)
            except Exception as e:
                raise TypeError("Builders must be picklable.") from e

    @typing.final
    def __eq__(self, other: typing.Any) -> bool:
        return type(self) is type(other) and self.fullname() == other.fullname()

    @typing.final
    def __hash__(self) -> int:
        return hash((type(self), self.fullname()))

    @property
    @typing.final
    def name(self) -> str:
        return self._name

    def fullname(self) -> str:
        return self._name

    @property
    @typing.final
    def builder(self: _T) -> Builder[_T] | None:
        return self._builder

    @property
    def env(self) -> Environment:
        assert self._env is not None
        return self._env

    @property
    def built(self) -> bool:
        return self._built

    @typing.final
    def build(self: _T) -> bool:
        _logger.debug(f"Building {self.fullname()}")

        if self._builder is None:
            if self.exists or self.dependencies:
                return True
            else:
                _logger.error(f"No rule to build {self.fullname()}.")
                return False

        different = (pwd := pathlib.Path.cwd()) != self.env.basedir
        if different:
            os.chdir(self.env.basedir)
        try:
            ret = self._builder(self)
        except Exception:
            _logger.exception(f"Exception caught while building {self.fullname()}")
            ret = False
        finally:
            if different:
                os.chdir(pwd)

        if ret:
            _logger.debug(f"Build for {self.fullname()} succeeded")
            self._built = True
        else:
            _logger.error(f"Build for {self.fullname()} failed")

        return ret

    @property
    def exists(self) -> bool:
        return False

    @property
    def last_modified(self) -> int | None:
        return None

    @functools.cached_property
    @typing.final
    def out_of_date(self) -> bool:
        if not self.exists:
            return True

        for dep in self.dependencies:
            if dep.out_of_date:
                return True

            if (
                self.last_modified is not None
                and dep.last_modified is not None
                and dep.last_modified > self.last_modified
            ):
                return True

        return False


@typing.final
class FileTarget(Target):
    def __init__(
        self: _T,
        name: str,
        *,
        dependencies: collections.abc.Iterable[Target] = (),
        builder: Builder[_T] | None = None,
    ) -> None:
        if isinstance(name, pathlib.Path):
            name = str(name)
        super().__init__(name, dependencies=dependencies, builder=builder)

    def _fullpath(self) -> pathlib.Path:
        return self.env.basedir / self.name

    def fullname(self) -> str:
        return str(self._fullpath())

    @functools.cached_property
    def exists(self) -> bool:
        return self._fullpath().exists()

    @functools.cached_property
    def last_modified(self) -> int | None:
        try:
            st = os.stat(self._fullpath())
        except FileNotFoundError:
            return None
        return int(st.st_mtime)


@typing.final
class Environment:
    def __init__(self, file: pathlib.Path | str, prev: Environment | None = None) -> None:
        if isinstance(file, str):
            file = pathlib.Path(file)
        if not file.is_dir():
            file = file.parent

        self.basedir = file.resolve()
        self._prev = prev
        self._map: dict[str, typing.Any] = {}
        self._targets: dict[str, Target] = {}
        self._clean_targets: list[Target] = []

        self._main_target: Target
        self.add_target(Target("", builder=_dummy_builder), main=True)

    def __repr__(self) -> str:
        return f"Environment(basedir={self.basedir}, {self._map})"

    @property
    def main_target(self) -> Target:
        return self._main_target

    @property
    def targets(self) -> dict[str, Target]:
        return {name: targ for name, targ in self._targets.items() if name}

    @property
    def clean_targets(self) -> list[Target]:
        return self._clean_targets.copy()

    def add_target(self, target: Target, *, main: bool = False, clean: bool = False) -> None:
        if main:
            self._main_target = target
        elif clean:
            env: Environment | None = self
            while env is not None:
                env._clean_targets.append(target)
                env = env._prev

        if target._env is None:
            target._env = self

        self._targets[target.name] = target

        for dep in target.dependencies:
            self.add_target(dep)

    def get(self, key: str, default: typing.Any = None) -> typing.Any:
        if (value := self._map.get(key, _sentinel)) is not _sentinel:
            return value

        if self._prev is not None:
            return self._prev.get(key, default)

        if (value := os.environ.get(key, _sentinel)) is not _sentinel:
            return value

        return default

    def __contains__(self, key: str) -> bool:
        return self.get(key, _sentinel) is not _sentinel

    def __getitem__(self, key: str) -> typing.Any:
        if (value := self.get(key, _sentinel)) is _sentinel:
            raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: typing.Any) -> None:
        self._map[key] = value

    def set_if_unset(self, key: str, value: typing.Any) -> None:
        if key not in self:
            self[key] = value

    def load_defaults(self, values: dict[str, typing.Any]) -> None:
        for key, value in values.items():
            self.set_if_unset(key, value)

    def load_subfile(self, directory: str | pathlib.Path) -> Environment | None:
        if isinstance(directory, str):
            directory = pathlib.Path(directory)

        module = importlib.import_module(str(directory / "Wormfile"))
        env = Environment(directory, prev=self)

        pwd = os.getcwd()
        os.chdir(directory)
        try:
            return env if module.load_targets(env) else None
        finally:
            os.chdir(pwd)
