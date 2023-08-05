#!/usr/bin/env python3

import argparse
import importlib
import os
import pathlib
import sys
import textwrap

from . import core
from . import target

VERSION = "0.1.0"


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true", help="Show the version and exit.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show verbose logging.")

    subparsers = parser.add_subparsers(dest="command")

    build_parser = subparsers.add_parser("build", help="Build a target.")
    build_parser.add_argument(
        "target", nargs="?", default="", help="The target to build.  Defaults to the main target."
    )

    subparsers.add_parser("clean", help="Clean the project.")

    subparsers.add_parser("init", help="Create a Wormfile.py template.")

    return parser.parse_args()


def make_template(dest: pathlib.Path) -> None:
    with dest.open("w") as f:
        f.write(textwrap.dedent("""
            #!/usr/bin/env python3

            import sandworm

            def load_targets(env: sandworm.Environment) -> bool:
                return False
            """))
    print("Wormfile.py created.")


def create_environment(args: argparse.Namespace) -> target.Environment | None:
    env = target.Environment(os.getcwd())
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


def main(args: argparse.Namespace) -> int:
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
    if (env := create_environment(args)) is None:
        return 1

    if args.command == "build":
        ret = do_build(env, args.target)
    else:
        ret = core.make_clean(env)
    return 0 if ret else 1


def _console_main() -> int:
    return main(get_args())
