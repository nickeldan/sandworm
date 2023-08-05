#!/usr/bin/env python3

import argparse
import importlib
import os
import pathlib
import sys
import textwrap

import sandworm


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


def create_environment(args: argparse.Namespace) -> sandworm.Environment | None:
    env = sandworm.Environment(os.getcwd())
    if args.command == "build":
        env["SANDWORM_TARGET"] = args.target
        env["SANDWORM_CLEAN"] = False
    else:
        env["SANDWORM_CLEAN"] = True

    if not importlib.import_module("Wormfile").load_targets(env):
        return None
    return env


def do_build(env: sandworm.Environment, target_str: str) -> int:
    if target_str:
        if (target := env.targets.get(target_str)) is None:
            print(f"No such target: {target_str}", file=sys.stderr)
            return 1
    else:
        if env.main_target is None:
            print("Main target not set.", file=sys.stderr)
            return 1
        target = env.main_target

    return 0 if sandworm.root_build(env, target) else 1


def do_clean(env: sandworm.Environment) -> int:
    return 0 if sandworm.make_clean(env) else 1


def main(args: argparse.Namespace) -> int:
    if args.version:
        print(sandworm.VERSION)
        return 0

    wormfile = pathlib.Path.cwd() / "Wormfile.py"
    if not wormfile.is_file():
        if args.command != "init":
            print("No Wormfile.py found.", file=sys.stderr)
            return 1
        make_template(wormfile)
        return 0

    sandworm.init_logging(verbose=args.verbose)
    if (env := create_environment(args)) is None:
        return 1

    if args.command == "build":
        ret = do_build(env, args.target)
    else:
        ret = do_clean(env)
    return ret


if __name__ == "__main__":
    sys.exit(main(get_args()))
