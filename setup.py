import multiprocessing
import os
import subprocess

from setuptools import setup


def compute_version():
    """Looks at latest tag, increases based on latest commit, outputs versions

    We don't actually invoke this from within setup, as that does not see the git repository.
    Rather, we invoke it externally, and provide the result via env variable.
    """
    latest_tag = subprocess.getoutput("git tag -l 'v*' | sort -V -r | head -n 1 | sed 's/^v\\(.*\\)/\\1/'")
    if not latest_tag:
        latest_tag = "0.0.0"
    latest_commit = subprocess.getoutput("git log -n 1 --oneline")
    vmaj, vmin, vmaint = [int(v) for v in latest_tag.split(".", 2)]
    if "MAJOR" in latest_commit:
        vmaj = vmaj + 1
        vmin = 0
        vmaint = 0
    elif "MINOR" in latest_commit:
        vmin = vmin + 1
        vmaint = 0
    else:
        vmaint = vmaint + 1
    version = f"{vmaj}.{vmin}.{vmaint}"
    return version


if __name__ == "__main__":
    # Freeze to support parallel compilation when using spawn instead of fork
    multiprocessing.freeze_support()
    setup(version=os.getenv("TARGET_VERSION"))
