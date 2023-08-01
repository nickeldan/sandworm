import collections.abc
import functools
import multiprocessing
import os
import pathlib
import time
import typing

from . import errors

T = typing.TypeVar("T", bound="Target")
Builder = typing.Callable[["Environment", T], bool]


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

    def __getitem__(self, key: str) -> typing.Any:
        sentinel = object()

        if (value := self._map.get(key, sentinel)) is not sentinel:
            return value

        if self._prev is not None:
            try:
                return self._prev[key]
            except KeyError:
                raise KeyError(key) from None

        if (value := os.environ.get(key, sentinel)) is not sentinel:
            return value

        raise KeyError(key)

    def __setitem__(self, key: str, value: typing.Any) -> None:
        self._map[key] = value
