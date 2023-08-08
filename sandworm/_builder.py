#!/usr/bin/env python3

import argparse
import importlib
import os
import pathlib
import re
import sys
import textwrap

from . import core
from . import target

VERSION = "0.1.0"


def get_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true", help="Show the version and exit.")

    subparsers = parser.add_subparsers(dest="command")

    build_parser = subparsers.add_parser("build", help="Build a target.")
    build_parser.add_argument(
        "target", nargs="?", default="", help="The target to build.  Defaults to the main target."
    )
    build_parser.add_argument("--verbose", "-v", action="store_true", help="Show verbose logging.")

    clean_parser = subparsers.add_parser("clean", help="Clean the project.")
    clean_parser.add_argument("--verbose", "-v", action="store_true", help="Show verbose logging.")

    subparsers.add_parser("init", help="Create a Wormfile.py template in the current directory.")

    return parser.parse_known_args()


def make_template(dest: pathlib.Path) -> None:
    with dest.open("w") as f:
        f.write(textwrap.dedent("""
            #!/usr/bin/env python3

            import sandworm

            def load_targets(env: sandworm.Environment) -> bool:
                return True
            """))
    print("Wormfile.py created.")


def create_environment(args: argparse.Namespace, extra_args: list[str]) -> target.Environment | None:
    env = target.Environment(os.getcwd())

    arg_pattern = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=(.+)")

    if args.command == "build" and (match := arg_pattern.match(args.target)):
        extra_args.append(args.target)
        args.target = ""

    for arg in extra_args:
        if not (match := arg_pattern.match(arg)):
            print(f"Invalid arg: {arg}", file=sys.stderr)
            return None
        key, value = match.groups()
        env[key] = value

    if args.command == "build":
        env["SANDWORM_TARGET"] = args.target
        env["SANDWORM_CLEAN"] = False
    else:
        env["SANDWORM_CLEAN"] = True

    sys.path.append(str(pathlib.Path.cwd()))
    if not importlib.import_module("Wormfile").load_targets(env):
        return None
    return env


def do_build(env: target.Environment, target_str: str) -> bool:
    if target_str:
        if (target := env.targets.get(target_str)) is None:
            print(f"No such target: {target_str}", file=sys.stderr)
            return False
    else:
        if env.main_target is None:
            print("Main target not set.", file=sys.stderr)
            return False
        target = env.main_target

    return core.root_build(env, target)


def main(args: argparse.Namespace, extra_args: list[str]) -> int:
    if args.version:
        print(VERSION)
        return 0

    wormfile = pathlib.Path.cwd() / "Wormfile.py"
    if not wormfile.is_file():
        if args.command != "init":
            print("No Wormfile.py found.", file=sys.stderr)
            return 1
        make_template(wormfile)
        return 0

    core.init_logging(verbose=args.verbose)
    if (env := create_environment(args, extra_args)) is None:
        return 1

    if args.command == "build":
        ret = do_build(env, args.target)
    else:
        ret = core.make_clean(env)
    return 0 if ret else 1


def _console_main() -> int:
    return main(*get_args())