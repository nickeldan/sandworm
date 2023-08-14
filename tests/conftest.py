import collections.abc
import os
import pathlib
import tempfile

import pytest

import sandworm


@pytest.fixture(autouse=True)
def workdir() -> collections.abc.Iterator[None]:
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp_dir:
        os.chdir(tmp_dir)
        try:
            yield
        finally:
            os.chdir(cwd)


@pytest.fixture
def env(workdir: None) -> sandworm.Environment:
    return sandworm.Environment(pathlib.Path.cwd())
