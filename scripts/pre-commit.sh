#!/bin/sh -e

black -q --check .
mypy .
flake8 .
