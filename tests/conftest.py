"""Sets a mock/fake of the S3 filesystem for any `fsql`-based usage."""
import os

import fsspec
import pytest
from moto import mock_s3


class Helper:
    def __init__(self):
        self.s3fs = fsspec.filesystem("s3")

    def put_s3_file(self, data, url):
        with self.s3fs.open(url, "wb") as fd:
            fd.write(data)


@pytest.fixture
def helper():
    return Helper()


@pytest.fixture(scope="function", autouse=True)
def mock_s3_utils_aws(monkeypatch):
    """Starts an s3 mock and yields it."""
    # this is probably overcomplicated -- remnant of when we had to monkeypatch the factory

    mock = mock_s3()
    mock.start()

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    yield mock
    mock.stop()
