"""Tests for the filer job specification of `Filer`."""

import os
from unittest.mock import patch

from tesk_core import path
from tesk_core.filer_class import Filer
from tesk_core.Util import pprint


@patch("tesk_core.path.HOST_BASE_PATH", "/home/tfga/workspace/cwl-tes")
@patch("tesk_core.path.CONTAINER_BASE_PATH", "/transfer")
@patch("tesk_core.path.TRANSFER_PVC_NAME", "transfer-pvc")
@patch.dict(
    os.environ,
    {
        "AWS_SHARED_CREDENTIALS_FILE": "/aws/credentials",
        "AWS_CONFIG_FILE": "/aws/config",
    },
)
class TestFilerClassEnv:
    """Tests with the base paths and the AWS files set."""

    def test_env_vars(self) -> None:
        """Test the environment variables and the backoff limit."""
        f = Filer("name", {"a": 1})
        f.set_backoffLimit(10)

        pprint(f.spec)

        assert f.getEnv() == [
            {"name": "JSON_INPUT", "value": '{"a": 1}'},
            {"name": "HOST_BASE_PATH", "value": "/home/tfga/workspace/cwl-tes"},
            {"name": "CONTAINER_BASE_PATH", "value": "/transfer"},
            {"name": "AWS_CONFIG_FILE", "value": "/aws/config"},
            {"name": "AWS_REQUEST_CHECKSUM_CALCULATION", "value": "WHEN_REQUIRED"},
            {"name": "AWS_RESPONSE_CHECKSUM_VALIDATION", "value": "WHEN_REQUIRED"},
            {"name": "AWS_SHARED_CREDENTIALS_FILE", "value": "/aws/credentials"},
        ]
        assert f.spec["spec"]["backoffLimit"] == 10

    def test_mounts(self) -> None:
        """Test the volume mounts and the volumes.

        The pod looks like this:

        kind: Pod
        apiVersion: v1
        metadata:
          name: tfga-pod
        spec:
          containers:
            - name: tfga-container
              image: eu.gcr.io/tes-wes/filer:testing
              volumeMounts:
                - mountPath: /transfer
                  name: transfer-volume
          volumes:
            - name: transfer-volume
              hostPath:
                path: /transferAtNode
              # persistentVolumeClaim:
              #  claimName: task-pv-claim
        """
        f = Filer("name", {"a": 1})

        pprint(f.spec)

        pprint(f.getVolumeMounts())

        assert f.getVolumeMounts() == [
            {"name": "tmp", "mountPath": "/tmp"},
            {
                "name": "transfer-volume",
                "mountPath": path.CONTAINER_BASE_PATH,
            },
            {"mountPath": "/aws", "name": "s3-conf", "readOnly": True},
        ]

        assert f.getVolumes() == [
            {"name": "tmp", "emptyDir": {}},
            {
                "name": "transfer-volume",
                "persistentVolumeClaim": {"claimName": "transfer-pvc"},
            },
            {
                "name": "s3-conf",
                "secret": {
                    "secretName": "aws-secret",
                    "items": [
                        {"key": "credentials", "path": "credentials"},
                        {"key": "config", "path": "config"},
                    ],
                    "optional": True,
                },
            },
        ]


@patch("tesk_core.path.HOST_BASE_PATH", None)
@patch("tesk_core.path.CONTAINER_BASE_PATH", None)
@patch("tesk_core.path.TRANSFER_PVC_NAME", None)
class TestFilerClassNoEnv:
    """Tests without the base paths, whatever the environment of the run."""

    def test_mounts_file_disabled(self) -> None:
        """Test that only the AWS volume is mounted without base paths."""
        f = Filer("name", {"a": 1})

        pprint(f.spec)

        pprint(f.getVolumeMounts())

        assert f.getVolumeMounts() == [
            {"name": "tmp", "mountPath": "/tmp"},
            {"mountPath": "/aws", "name": "s3-conf", "readOnly": True},
        ]
        assert f.getVolumes() == [
            {"name": "tmp", "emptyDir": {}},
            {
                "name": "s3-conf",
                "secret": {
                    "secretName": "aws-secret",
                    "items": [
                        {"key": "credentials", "path": "credentials"},
                        {"key": "config", "path": "config"},
                    ],
                    "optional": True,
                },
            },
        ]

    def test_image_pull_policy(self) -> None:
        """Test the image pull policy with and without `pullPolicyAlways`."""
        f = Filer("name", {"a": 1})
        assert f.getImagePullPolicy() == "IfNotPresent"

        f = Filer("name", {"a": 1}, pullPolicyAlways=True)
        assert f.getImagePullPolicy() == "Always"


@patch.dict(os.environ, {"TESK_FTP_HOSTS": "ftp.example.org,[::1]:2121"})
def test_set_ftp_passes_hosts() -> None:
    """Test that the FTP credentials are passed with the taskmaster's hosts."""
    f = Filer("name", {"a": 1})
    f.set_ftp("user", "pass")

    variables = {var["name"]: var["value"] for var in f.getEnv()}
    assert variables["TESK_FTP_USERNAME"] == "user"
    assert variables["TESK_FTP_PASSWORD"] == "pass"  # noqa: S105
    assert variables["TESK_FTP_HOSTS"] == "ftp.example.org,[::1]:2121"


@patch.dict(os.environ, clear=True)
def test_set_ftp_without_hosts() -> None:
    """Test that only the credentials are passed without hosts."""
    f = Filer("name", {"a": 1})
    f.set_ftp("user", "pass")

    names = [var["name"] for var in f.getEnv()]
    assert "TESK_FTP_USERNAME" in names
    assert "TESK_FTP_HOSTS" not in names
