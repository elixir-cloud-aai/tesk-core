"""The manifest of a filer job."""

import copy
import json
import logging
import os
import posixpath
from typing import TYPE_CHECKING, Any

from kubernetes import client

from tesk_core import path
from tesk_core.path import fileEnabled

if TYPE_CHECKING:
    from tesk_core.pvc import PVC


TASK_VOLUME = "task-volume"
"""Name of the volume of the task's claim in the filer's pod."""

FILER_LABELS = {
    "app.kubernetes.io/component": "filer",
    "app.kubernetes.io/part-of": "tesk",
}
"""Labels of the filer's pods."""

TMP_VOLUME = "tmp"
"""Name of the `emptyDir` volume at `/tmp` in the filer's pod."""

DEFAULT_POD_SECURITY_CONTEXT: dict[str, Any] = {
    "runAsNonRoot": True,
    "seccompProfile": {"type": "RuntimeDefault"},
}
"""Security context of the filer's pod unless `FILER_POD_SECURITY_CONTEXT`
is set. With the container's (`DEFAULT_SECURITY_CONTEXT`), the pod meets the
`restricted` Pod Security Standard; the filer image runs as user 100."""

DEFAULT_SECURITY_CONTEXT: dict[str, Any] = {
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
    "readOnlyRootFilesystem": True,
}
"""Security context of the filer's container unless `FILER_SECURITY_CONTEXT`
is set. The filer writes only to its volumes, `/tmp` included."""

logger = logging.getLogger(__name__)


def _object_setting(
    name: str, model: type, default: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Get a Kubernetes object from a JSON setting in the environment.

    Like `taskmaster.env_whole_number`, logs invalid values and falls back
    to the default.

    Args:
        name: The environment variable.
        model: The Kubernetes client's model that the object must be valid
            for (e.g. `client.V1SecurityContext`); checked with clients that
            have pydantic models (37 and later), otherwise only by the API
            server.
        default: The object if the setting is unset, empty or invalid.

    Returns:
        The object, or a copy of the default; None for `{}`, or for an unset,
        empty or invalid setting without default. Invalid settings are
        logged.
    """
    raw = os.environ.get(name, "")
    if not raw.strip():
        return copy.deepcopy(default)
    try:
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("not a JSON object")
        validate = getattr(model, "model_validate", None)
        if validate is not None:
            # The models also take snake_case fields, which the API server
            # would drop: the manifest gets the model's camelCase ones
            value = validate(value).model_dump(
                by_alias=True, exclude_none=True, mode="json"
            )
    except ValueError as error:
        # `json.JSONDecodeError` and pydantic's `ValidationError` are
        # `ValueError`s; only the first line of the error is logged (the
        # pydantic error's has no values)
        logger.error(
            "Ignoring %s: not a JSON object of a valid %s (%s); using the default",
            name,
            model.__name__,
            str(error).splitlines()[0],
        )
        return copy.deepcopy(default)
    return value or None


def _set_field(target: dict[str, Any], key: str, value: Any) -> None:
    """Set a field of a manifest, or remove it.

    Args:
        target: The manifest, e.g. a container.
        key: The field.
        value: Its value; None removes the field.
    """
    if value is None:
        target.pop(key, None)
    else:
        target[key] = value


class Filer:
    """The manifest of a filer job, which stages a task's inputs or outputs.

    Attributes:
        name: Name of the job.
        json_pvc: Name of the ConfigMap with the gzipped task, if any;
            otherwise the task is passed in the environment.
        spec: The manifest of the job.
    """

    def getVolumes(self) -> list[dict[str, Any]]:
        """Get the volumes of the job's pod.

        Returns:
            The volumes.
        """
        volumes: list[dict[str, Any]] = self.spec["spec"]["template"]["spec"]["volumes"]
        return volumes

    def getContainer(self, i: int) -> dict[str, Any]:
        """Get a container of the job's pod.

        Args:
            i: Index of the container.

        Returns:
            The container.
        """
        container: dict[str, Any] = self.spec["spec"]["template"]["spec"]["containers"][
            i
        ]
        return container

    def getVolumeMounts(self) -> list[dict[str, Any]]:
        """Get the volume mounts of the filer's container.

        Returns:
            The volume mounts.
        """
        mounts: list[dict[str, Any]] = self.getContainer(0)["volumeMounts"]
        return mounts

    def getEnv(self) -> list[dict[str, Any]]:
        """Get the environment variables of the filer's container.

        Returns:
            The environment variables.
        """
        env: list[dict[str, Any]] = self.getContainer(0)["env"]
        return env

    def getImagePullPolicy(self) -> str:
        """Get the image pull policy of the filer's container.

        Returns:
            The image pull policy.
        """
        policy: str = self.getContainer(0)["imagePullPolicy"]
        return policy

    def __init__(
        self,
        name: str,
        data: dict[str, Any],
        filer_name: str = "eu.gcr.io/tes-wes/filer",
        filer_version: str = "v0.5",
        pullPolicyAlways: bool = False,
        json_pvc: str | None = None,
    ) -> None:
        """Describe the filer job.

        Args:
            name: Name of the job.
            data: The task, passed in the environment if `json_pvc` is not
                set.
            filer_name: Image of the filer, without tag.
            filer_version: Tag of the filer image.
            pullPolicyAlways: Whether to always pull the image.
            json_pvc: Name of the ConfigMap with the gzipped task, if any.

        Raises:
            ValueError: `CONTAINER_BASE_PATH` is inside or contains
                `path.TASK_PATH_PREFIX`.
        """
        self.name = name
        self.json_pvc = json_pvc
        self.spec: dict[str, Any] = {
            "kind": "Job",
            "apiVersion": "batch/v1",
            "metadata": {"name": name},
            "spec": {
                "template": {
                    # The labels select filer pods, e.g. in a NetworkPolicy
                    "metadata": {"name": "tesk-filer", "labels": dict(FILER_LABELS)},
                    "spec": {
                        "containers": [
                            {
                                "name": "filer",
                                "image": f"{filer_name}:{filer_version}",
                                "args": [],
                                "env": [],
                                "volumeMounts": [],
                                "imagePullPolicy": "Always"
                                if pullPolicyAlways
                                else "IfNotPresent",
                            }
                        ],
                        "volumes": [],
                        "restartPolicy": "Never",
                        # The filer doesn't use the Kubernetes API
                        "automountServiceAccountToken": False,
                    },
                }
            },
        }

        self.set_hardening()

        env = self.getEnv()
        if json_pvc is None:
            env.append({"name": "JSON_INPUT", "value": json.dumps(data)})
        env.append({"name": "HOST_BASE_PATH", "value": path.HOST_BASE_PATH})
        env.append({"name": "CONTAINER_BASE_PATH", "value": path.CONTAINER_BASE_PATH})
        # The filer's HTTP(S) timeout, if the taskmaster has one (as
        # `filer.HTTP_TIMEOUT_ENV`, not imported: the taskmaster doesn't
        # need the filer's dependencies)
        http_timeout = os.environ.get("FILER_HTTP_TIMEOUT")
        if http_timeout is not None:
            env.append({"name": "FILER_HTTP_TIMEOUT", "value": http_timeout})

        if json_pvc:
            self.getVolumeMounts().append(
                {"name": "jsoninput", "mountPath": "/jsoninput"}
            )
            self.getVolumes().append(
                {"name": "jsoninput", "configMap": {"name": json_pvc}}
            )

        if fileEnabled():
            path.checkBasePaths()
            self.getVolumeMounts().append(
                {"name": "transfer-volume", "mountPath": path.CONTAINER_BASE_PATH}
            )

            self.getVolumes().append(
                {
                    "name": "transfer-volume",
                    "persistentVolumeClaim": {"claimName": path.TRANSFER_PVC_NAME},
                }
            )

        self.add_tmp_mount()
        self.add_s3_mount()

    def add_tmp_mount(self) -> None:
        """Mount a writable `/tmp` (an `emptyDir`) on the read-only root.

        Not if another volume is mounted there already (the shared volume of
        `file:`, with `CONTAINER_BASE_PATH` at `/tmp`), as mount paths must be
        unique.
        """
        tmp = "/tmp"  # noqa: S108 -- an emptyDir in the filer's pod
        if any(
            posixpath.normpath(mount["mountPath"]) == tmp
            for mount in self.getVolumeMounts()
        ):
            return
        self.getVolumeMounts().append({"name": TMP_VOLUME, "mountPath": tmp})
        self.getVolumes().append({"name": TMP_VOLUME, "emptyDir": {}})

    def set_hardening(self) -> None:
        """Set the security contexts and the resources of the filer's pod.

        From the settings `FILER_POD_SECURITY_CONTEXT`,
        `FILER_SECURITY_CONTEXT` and `FILER_RESOURCES` (JSON objects) in the
        taskmaster's environment. Each replaces its default (the security
        contexts `DEFAULT_POD_SECURITY_CONTEXT` and
        `DEFAULT_SECURITY_CONTEXT`, no resources); `{}` leaves the field out.
        """
        pod = self.spec["spec"]["template"]["spec"]
        container = self.getContainer(0)
        _set_field(
            pod,
            "securityContext",
            _object_setting(
                "FILER_POD_SECURITY_CONTEXT",
                client.V1PodSecurityContext,
                DEFAULT_POD_SECURITY_CONTEXT,
            ),
        )
        _set_field(
            container,
            "securityContext",
            _object_setting(
                "FILER_SECURITY_CONTEXT",
                client.V1SecurityContext,
                DEFAULT_SECURITY_CONTEXT,
            ),
        )
        _set_field(
            container,
            "resources",
            _object_setting("FILER_RESOURCES", client.V1ResourceRequirements, None),
        )

    def add_s3_mount(self) -> None:
        """Mount the S3 configuration from the secret `aws-secret`, if it exists."""
        env = self.getEnv()
        env.append({"name": "AWS_CONFIG_FILE", "value": "/aws/config"})
        env.append(
            {"name": "AWS_REQUEST_CHECKSUM_CALCULATION", "value": "WHEN_REQUIRED"}
        )
        env.append(
            {"name": "AWS_RESPONSE_CHECKSUM_VALIDATION", "value": "WHEN_REQUIRED"}
        )
        env.append({"name": "AWS_SHARED_CREDENTIALS_FILE", "value": "/aws/credentials"})

        self.getVolumeMounts().append(
            {
                "name": "s3-conf",
                "mountPath": "/aws",
                "readOnly": True,
            }
        )
        self.getVolumes().append(
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
            }
        )

    def set_ftp(self, user: str | None, pw: str | None) -> None:
        """Pass FTP credentials to the filer.

        If `TESK_FTP_SECRET_NAME` is set in the taskmaster's environment,
        the filer's environment references the keys `username` and
        `password` of that Kubernetes secret, and holds no credentials;
        otherwise, it holds them as values, and a warning is logged.

        Also passes `TESK_FTP_HOSTS`, the hosts that the filer may send them
        to, from the taskmaster's environment, if it is set there.

        Args:
            user: The FTP user name.
            pw: The FTP password.
        """
        env = self.getEnv()
        secret = os.environ.get("TESK_FTP_SECRET_NAME", "").strip()
        if secret:
            for var, key in (
                ("TESK_FTP_USERNAME", "username"),
                ("TESK_FTP_PASSWORD", "password"),
            ):
                env.append(
                    {
                        "name": var,
                        "valueFrom": {"secretKeyRef": {"name": secret, "key": key}},
                    }
                )
        else:
            logger.warning(
                "Passing the FTP credentials to the filer as values in its job;"
                " set TESK_FTP_SECRET_NAME to the name of their secret to pass"
                " references instead"
            )
            env.append({"name": "TESK_FTP_USERNAME", "value": user})
            env.append({"name": "TESK_FTP_PASSWORD", "value": pw})
        hosts = os.environ.get("TESK_FTP_HOSTS")
        if hosts is not None:
            env.append({"name": "TESK_FTP_HOSTS", "value": hosts})

    def set_backoffLimit(self, limit: int) -> None:
        """Set the number of retries of the job (Kubernetes' default is 6).

        The taskmaster sets it from the environment variable
        `FILER_BACKOFF_LIMIT`.

        Args:
            limit: The number of retries before considering a Job as failed.
        """
        self.spec["spec"].update({"backoffLimit": limit})

    def add_volume_mount(self, pvc: "PVC") -> None:
        """Mount the task's paths on its persistent volume claim.

        Each path is mounted below `path.TASK_PATH_PREFIX` (see
        `path.filerPath`), where the filer reads and writes it.

        Args:
            pvc: The claim, with its volume mounts set.
        """
        # Copies: `get_spec` changes them, and the executors mount the claim's
        # mounts at the task's paths
        self.getVolumeMounts().extend(
            {**mount, "mountPath": path.filerPath(mount["mountPath"])}
            for mount in pvc.volume_mounts
        )
        self.getVolumes().append(
            {"name": TASK_VOLUME, "persistentVolumeClaim": {"claimName": pvc.name}}
        )

    def add_netrc_mount(self, netrc_name: str | None = "netrc") -> None:
        """Mount a `.netrc` file from a secret as `$HOME/.netrc`.

        Sets `$HOME` to `/opt/home`, so that it doesn't depend on the user
        that the container runs as.

        Args:
            netrc_name: Name of the secret, with the key `.netrc`.
        """
        self.getVolumeMounts().append(
            {"name": "netrc", "mountPath": "/opt/home/.netrc", "subPath": ".netrc"}
        )
        self.getVolumes().append(
            {
                "name": "netrc",
                "secret": {
                    "secretName": netrc_name,
                    "defaultMode": 420,
                    "items": [{"key": ".netrc", "path": ".netrc"}],
                },
            }
        )
        self.getEnv().append({"name": "HOME", "value": "/opt/home"})

    def get_spec(self, mode: str, debug: bool = False) -> dict[str, Any]:
        """Get the manifest of the job.

        The task's paths are mounted read-only for `outputs`.

        Args:
            mode: `inputs` or `outputs`.
            debug: Whether the filer logs debug messages.

        Returns:
            The manifest.
        """
        if self.json_pvc is None:
            self.spec["spec"]["template"]["spec"]["containers"][0]["args"] = [
                mode,
                "$(JSON_INPUT)",
            ]
        else:
            self.spec["spec"]["template"]["spec"]["containers"][0]["args"] = [
                mode,
                "/jsoninput/JSON_INPUT.gz",
            ]

        if debug:
            self.spec["spec"]["template"]["spec"]["containers"][0]["args"].append("-d")

        # The outputs filer only reads the task's paths
        for mount in self.getVolumeMounts():
            if mount["name"] == TASK_VOLUME:
                if mode == "outputs":
                    mount["readOnly"] = True
                else:
                    mount.pop("readOnly", None)

        self.spec["spec"]["template"]["metadata"]["name"] = self.name
        return self.spec
