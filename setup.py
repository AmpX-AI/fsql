import multiprocessing
import os

from setuptools import setup

if __name__ == "__main__":
    # Freeze to support parallel compilation when using spawn instead of fork
    multiprocessing.freeze_support()
    setup(version=os.getenv("TARGET_VERSION"))
