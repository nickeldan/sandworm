from __future__ import annotations
import collections.abc
import functools
import importlib
import multiprocessing
import os
import pathlib
import typing

from . import errors

T = typing.TypeVar("T", bound="Target")
Builder = typing.Callable[["Environment", T], bool]

_sentinel = object()


class Target:
    def __init__(
        self: T,
        name: str,
        env: Environment,
        dependencies: collections.abc.Iterable[Target],
        builder: Builder[T],
    ) -> None:
        self._name = name
        self._origin = env.file
        self._dependencies = list(dependencies)
        self._builder = builder
        self._build_event = multiprocessing.Event()

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
    def basedir(self) -> pathlib.Path:
        return self._origin.parent

    @property
    @typing.final
    def origin(self) -> pathlib.Path:
        return self._origin

    @property
    @typing.final
    def dependencies(self) -> list[Target]:
        return self._dependencies

    @property
    @typing.final
    def builder(self: T) -> Builder[T]:
        return self._builder

    def built(self) -> bool:
        return self._build_event.wait()

    def set_built(self) -> None:
        self._build_event.set()

    @functools.cached_property
    def exists(self) -> bool:
        return False

    @functools.cached_property
    def last_modified(self) -> typing.Optional[int]:
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
    def __init__(self, *args: typing.Any) -> None:
        super().__init__(*args)
        self._fullpath = (self.basedir / self.name).resolve()

    def fullname(self) -> str:
        return str(self._fullpath)

    @functools.cached_property
    def exists(self) -> bool:
        return self._fullpath.exists()

    @functools.cached_property
    def last_modified(self) -> typing.Optional[int]:
        try:
            st = os.stat(self._fullpath)
        except FileNotFoundError:
            return None
        return int(st.st_mtime)


@typing.final
class Environment:
    def __init__(self, file: pathlib.Path, prev: Environment | None = None) -> None:
        self._file = file.resolve()
        self._prev = prev
        self._map: dict[str, typing.Any] = {}
        self._targets: dict[str, Target] = {}

    @property
    def file(self) -> pathlib.Path:
        return self._file

    def add_target(self, target: Target) -> None:
        if target.name in self._targets:
            raise errors.RepeatedTargetError(target.name)
        self._targets[target.name] = target

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

    def load_defaults(self, values: dict[str, typing.Any]) -> None:
        for key, value in values.items():
            if key not in self:
                self[key] = value

    def load_subfile(self, directory: str | pathlib.Path) -> Environment:
        if isinstance(directory, str):
            directory = pathlib.Path(directory)

        module = importlib.import_module(str(directory / "Wormfile"))
        env = Environment(directory / "Wormfile.py", self)

        pwd = os.getcwd()
        os.chdir(directory)
        try:
            module.load_targets(env)
        finally:
            os.chdir(pwd)
        return env
