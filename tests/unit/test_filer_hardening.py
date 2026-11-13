"""Tests of the filer pod's hardening and of how it gets the FTP credentials."""

import logging
from typing import Any

from kubernetes import client
import pytest

from tesk_core import filer_class, path
from tesk_core.filer_class import (
    DEFAULT_POD_SECURITY_CONTEXT,
    DEFAULT_SECURITY_CONTEXT,
    Filer,
)

SETTINGS = (
    "FILER_POD_SECURITY_CONTEXT",
    "FILER_SECURITY_CONTEXT",
    "FILER_RESOURCES",
    "TESK_FTP_SECRET_NAME",
    "TESK_FTP_HOSTS",
)


@pytest.fixture(autouse=True)
def no_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset the settings of the filer pod in the environment.

    Args:
        monkeypatch: The fixture that restores the environment afterwards.
    """
    for name in SETTINGS:
        monkeypatch.delenv(name, raising=False)


def pod_spec(filer: Filer) -> dict[str, Any]:
    """Get the spec of the filer's pod.

    Args:
        filer: The filer job.

    Returns:
        The pod spec.
    """
    spec: dict[str, Any] = filer.spec["spec"]["template"]["spec"]
    return spec


def env_entry(filer: Filer, name: str) -> dict[str, Any]:
    """Get an environment entry of the filer's container.

    Args:
        filer: The filer job.
        name: Name of the variable.

    Returns:
        The entry.
    """
    return next(var for var in filer.getEnv() if var["name"] == name)


def test_default_hardening() -> None:
    """Test that the filer pod meets the `restricted` Pod Security Standard."""
    filer = Filer("name", {"a": 1})

    spec = pod_spec(filer)
    assert spec["securityContext"] == {
        "runAsNonRoot": True,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert filer.getContainer(0)["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    assert spec["automountServiceAccountToken"] is False
    assert "resources" not in filer.getContainer(0)


def test_pod_labels() -> None:
    """Test that filer pods have labels to select them by, as documented."""
    filer = Filer("name", {"a": 1})

    labels = filer.get_spec("inputs")["spec"]["template"]["metadata"]["labels"]
    assert labels == {
        "app.kubernetes.io/component": "filer",
        "app.kubernetes.io/part-of": "tesk",
    }


def test_writable_tmp() -> None:
    """Test that the filer gets a writable `/tmp` on its read-only root."""
    filer = Filer("name", {"a": 1})

    assert {"name": "tmp", "mountPath": "/tmp"} in filer.getVolumeMounts()
    assert {"name": "tmp", "emptyDir": {}} in filer.getVolumes()


@pytest.mark.parametrize("base", ["/tmp", "/tmp/"])
def test_tmp_is_the_shared_volume(monkeypatch: pytest.MonkeyPatch, base: str) -> None:
    """Test that `/tmp` isn't mounted twice if the shared volume is there.

    Kubernetes rejects pods with two mounts at one path.
    """
    monkeypatch.setattr(path, "HOST_BASE_PATH", "/data")
    monkeypatch.setattr(path, "CONTAINER_BASE_PATH", base)
    monkeypatch.setattr(path, "TRANSFER_PVC_NAME", "transfer-pvc")

    filer = Filer("name", {"a": 1})

    names = [mount["name"] for mount in filer.getVolumeMounts()]
    assert "transfer-volume" in names
    assert "tmp" not in names
    assert all(volume["name"] != "tmp" for volume in filer.getVolumes())


def test_tmp_below_the_shared_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that `/tmp` is mounted with a shared volume below it."""
    monkeypatch.setattr(path, "HOST_BASE_PATH", "/data")
    monkeypatch.setattr(path, "CONTAINER_BASE_PATH", "/tmp/transfer")
    monkeypatch.setattr(path, "TRANSFER_PVC_NAME", "transfer-pvc")

    filer = Filer("name", {"a": 1})

    assert {"name": "tmp", "mountPath": "/tmp"} in filer.getVolumeMounts()


def test_snake_case_fields_become_camel_case(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the manifest gets the fields that the API server knows.

    The client's models also take snake_case names, which the API server
    would drop with a warning only.
    """
    if not hasattr(client.V1SecurityContext, "model_validate"):
        pytest.skip("the Kubernetes client has no pydantic models (before 37)")
    monkeypatch.setenv(
        "FILER_SECURITY_CONTEXT",
        '{"run_as_user": 1000, "read_only_root_filesystem": true,'
        ' "seccomp_profile": {"type": "RuntimeDefault"}}',
    )

    filer = Filer("name", {"a": 1})

    assert filer.getContainer(0)["securityContext"] == {
        "runAsUser": 1000,
        "readOnlyRootFilesystem": True,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def test_defaults_are_copies() -> None:
    """Test that changing one filer's contexts changes no other's."""
    filer = Filer("name", {"a": 1})
    filer.getContainer(0)["securityContext"]["capabilities"]["drop"].append("X")
    pod_spec(filer)["securityContext"]["seccompProfile"]["type"] = "X"

    other = Filer("other", {"a": 1})

    assert other.getContainer(0)["securityContext"] == DEFAULT_SECURITY_CONTEXT
    assert pod_spec(other)["securityContext"] == DEFAULT_POD_SECURITY_CONTEXT
    assert DEFAULT_SECURITY_CONTEXT["capabilities"] == {"drop": ["ALL"]}


def test_contexts_and_resources_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the settings replace the contexts and set the resources."""
    monkeypatch.setenv(
        "FILER_POD_SECURITY_CONTEXT", ' {"runAsUser": 1000, "fsGroup": 1000} '
    )
    monkeypatch.setenv("FILER_SECURITY_CONTEXT", '{"readOnlyRootFilesystem": false}')
    monkeypatch.setenv(
        "FILER_RESOURCES",
        '{"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "1Gi"}}',
    )

    filer = Filer("name", {"a": 1})

    assert pod_spec(filer)["securityContext"] == {"runAsUser": 1000, "fsGroup": 1000}
    assert filer.getContainer(0)["securityContext"] == {"readOnlyRootFilesystem": False}
    assert filer.getContainer(0)["resources"] == {
        "requests": {"cpu": "100m", "memory": "128Mi"},
        "limits": {"memory": "1Gi"},
    }


def test_empty_object_removes_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that `{}` leaves a context out, e.g. for the cluster's defaults."""
    monkeypatch.setenv("FILER_POD_SECURITY_CONTEXT", "{}")
    monkeypatch.setenv("FILER_SECURITY_CONTEXT", "{}")
    monkeypatch.setenv("FILER_RESOURCES", "{}")

    filer = Filer("name", {"a": 1})

    assert "securityContext" not in pod_spec(filer)
    assert "securityContext" not in filer.getContainer(0)
    assert "resources" not in filer.getContainer(0)


@pytest.mark.parametrize("value", ["", "  "])
def test_empty_setting_keeps_default(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """Test that an empty setting counts as unset."""
    monkeypatch.setenv("FILER_POD_SECURITY_CONTEXT", value)
    monkeypatch.setenv("FILER_SECURITY_CONTEXT", value)
    monkeypatch.setenv("FILER_RESOURCES", value)

    filer = Filer("name", {"a": 1})

    assert pod_spec(filer)["securityContext"] == DEFAULT_POD_SECURITY_CONTEXT
    assert filer.getContainer(0)["securityContext"] == DEFAULT_SECURITY_CONTEXT
    assert "resources" not in filer.getContainer(0)


@pytest.mark.parametrize("value", ["runAsNonRoot: true", "[]", '"text"', "null"])
def test_setting_not_an_object_keeps_default(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, value: str
) -> None:
    """Test that a setting that isn't a JSON object logs an error.

    The defaults stay.
    """
    for name in SETTINGS[:3]:
        monkeypatch.setenv(name, value)

    with caplog.at_level(logging.ERROR):
        filer = Filer("name", {"a": 1})

    assert pod_spec(filer)["securityContext"] == DEFAULT_POD_SECURITY_CONTEXT
    assert filer.getContainer(0)["securityContext"] == DEFAULT_SECURITY_CONTEXT
    assert "resources" not in filer.getContainer(0)
    for name in SETTINGS[:3]:
        assert any(name in record.getMessage() for record in caplog.records), name


@pytest.mark.parametrize(
    ("name", "model", "value"),
    [
        # Wrong types and unknown fields, of each setting's own model
        ("FILER_POD_SECURITY_CONTEXT", client.V1PodSecurityContext, '{"fsGroup": "x"}'),
        (
            "FILER_POD_SECURITY_CONTEXT",
            client.V1PodSecurityContext,
            '{"runAsNonroot": true}',
        ),
        (
            "FILER_POD_SECURITY_CONTEXT",
            client.V1PodSecurityContext,
            '{"allowPrivilegeEscalation": false}',
        ),
        ("FILER_SECURITY_CONTEXT", client.V1SecurityContext, '{"privileged": "maybe"}'),
        ("FILER_SECURITY_CONTEXT", client.V1SecurityContext, '{"fsGroup": 1000}'),
        ("FILER_RESOURCES", client.V1ResourceRequirements, '{"limits": 5}'),
        ("FILER_RESOURCES", client.V1ResourceRequirements, '{"limit": {"cpu": "1"}}'),
    ],
)
def test_setting_invalid_for_model_keeps_default(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    name: str,
    model: type,
    value: str,
) -> None:
    """Test that an object that isn't valid for its model logs an error.

    Checked with the Kubernetes client's pydantic models (from 37); the
    default stays.
    """
    if not hasattr(model, "model_validate"):
        pytest.skip("the Kubernetes client has no pydantic models (before 37)")
    monkeypatch.setenv(name, value)

    with caplog.at_level(logging.ERROR):
        filer = Filer("name", {"a": 1})

    assert pod_spec(filer)["securityContext"] == DEFAULT_POD_SECURITY_CONTEXT
    assert filer.getContainer(0)["securityContext"] == DEFAULT_SECURITY_CONTEXT
    assert "resources" not in filer.getContainer(0)
    assert name in caplog.text
    assert model.__name__ in caplog.text


def test_unvalidated_without_pydantic_models(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that objects are only parsed with models without `model_validate`.

    Kubernetes clients before 37 have no pydantic models; the API server
    checks the fields then.
    """

    class Model:
        """A model without `model_validate`."""

    monkeypatch.setenv("SOME_SETTING", '{"unknownField": 1}')

    assert filer_class._object_setting("SOME_SETTING", Model, None) == {
        "unknownField": 1
    }


def test_ftp_credentials_from_secret(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the filer references the secret, without the credentials."""
    monkeypatch.setenv("TESK_FTP_SECRET_NAME", " tesk-ftp ")
    filer = Filer("name", {"a": 1})

    with caplog.at_level(logging.WARNING):
        filer.set_ftp("ftp-user-1", "ftp-pw-1")

    assert env_entry(filer, "TESK_FTP_USERNAME") == {
        "name": "TESK_FTP_USERNAME",
        "valueFrom": {"secretKeyRef": {"name": "tesk-ftp", "key": "username"}},
    }
    assert env_entry(filer, "TESK_FTP_PASSWORD") == {
        "name": "TESK_FTP_PASSWORD",
        "valueFrom": {"secretKeyRef": {"name": "tesk-ftp", "key": "password"}},
    }
    assert "ftp-user-1" not in str(filer.spec)
    assert "ftp-pw-1" not in str(filer.spec)
    assert not caplog.records


@pytest.mark.parametrize("secret", [None, "", " "])
def test_ftp_credentials_as_values(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    secret: str | None,
) -> None:
    """Test that the credentials are values without a secret, with a warning."""
    if secret is not None:
        monkeypatch.setenv("TESK_FTP_SECRET_NAME", secret)
    filer = Filer("name", {"a": 1})

    with caplog.at_level(logging.WARNING):
        filer.set_ftp("ftp-user-1", "ftp-pw-1")

    assert env_entry(filer, "TESK_FTP_USERNAME")["value"] == "ftp-user-1"
    assert env_entry(filer, "TESK_FTP_PASSWORD")["value"] == "ftp-pw-1"
    assert "TESK_FTP_SECRET_NAME" in caplog.text
    assert "ftp-pw-1" not in caplog.text
