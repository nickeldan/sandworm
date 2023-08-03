#!/bin/sh -e

black -q --check sandworm
mypy sandworm
flake8 sandworm