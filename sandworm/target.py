from __future__ import annotations
import collections.abc
import functools
import importlib
import logging
import os
import pathlib
import typing

from . import errors

_T = typing.TypeVar("_T", bound="Target")
_Builder = typing.Callable[[_T], bool]

_logger = logging.getLogger("sandworm.target")
_clean_targets: list[Target] = []

_sentinel = object()


class Target:
    def __init__(
        self: _T,
        name: str,
        dependencies: collections.abc.Iterable[Target] = (),
        builder: _Builder[_T] | None = None,
    ) -> None:
        self._name = name
        self._dependencies = list(dependencies)
        self._builder = builder
        self._env: Environment | None = None
        self._built = False

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
    def dependencies(self) -> list[Target]:
        return self._dependencies

    @property
    def env(self) -> Environment:
        if self._env is None:
            raise errors.NoEnvironmentError(f"The {self._name} target has not been added to an environment.")
        return self._env

    @property
    def built(self) -> bool:
        return self._built

    @typing.final
    def build(self: _T) -> bool:
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
        finally:
            if different:
                os.chdir(pwd)
        if ret:
            self._built = True
        return ret

    @functools.cached_property
    def exists(self) -> bool:
        return False

    @functools.cached_property
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
        self._main_target: Target | None = None

    @property
    def targets(self) -> dict[str, Target]:
        return self._targets

    @property
    def main_target(self) -> Target | None:
        return self._main_target

    def add_target(self, target: Target, *, main: bool = False, clean: bool = False) -> None:
        if target.name in self._targets:
            return

        if main:
            if self._main_target is not None:
                raise errors.SecondMainTargetError(target.name)
            self._main_target = target

        if clean:
            _clean_targets.append(target)

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

    def load_subfile(self, directory: str | pathlib.Path) -> Environment:
        if isinstance(directory, str):
            directory = pathlib.Path(directory)

        module = importlib.import_module(str(directory / "Wormfile"))
        env = Environment(directory, prev=self)

        pwd = os.getcwd()
        os.chdir(directory)
        try:
            module.load_targets(env)
        finally:
            os.chdir(pwd)
        return env
