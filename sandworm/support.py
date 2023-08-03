import subprocess
import typing


def c_support() -> dict[str, typing.Any]:
    values: dict[str, typing.Any] = {
        "CPPFLAGS": [],
        "CFLAGS": [],
        "LDFLAGS": [],
    }

    for compiler in ("cc", "gcc", "clang"):
        p = subprocess.run(f"which {compiler}", shell=True, text=True, stdout=subprocess.PIPE)
        if p.returncode == 0:
            values["CC"] = p.stdout.rstrip()
            break
    else:
        raise FileNotFoundError("Could not locate C compiler.")

    for cmd in ("ld", "ar", "as"):
        p = subprocess.run(f"which {cmd}", shell=True, text=True, stdout=subprocess.PIPE)
        if p.returncode != 0:
            raise FileNotFoundError(f"Could not locate {cmd}.")
        values[cmd.upper()] = p.stdout.rstrip()

    return values
