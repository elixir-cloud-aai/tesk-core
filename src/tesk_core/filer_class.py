"""The manifest of a filer job."""

import json
import os
from typing import TYPE_CHECKING, Any

from tesk_core import path
from tesk_core.path import fileEnabled

if TYPE_CHECKING:
    from tesk_core.pvc import PVC


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
        """
        self.name = name
        self.json_pvc = json_pvc
        self.spec: dict[str, Any] = {
            "kind": "Job",
            "apiVersion": "batch/v1",
            "metadata": {"name": name},
            "spec": {
                "template": {
                    "metadata": {"name": "tesk-filer"},
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
                    },
                }
            },
        }

        env = self.getEnv()
        if json_pvc is None:
            env.append({"name": "JSON_INPUT", "value": json.dumps(data)})
        env.append({"name": "HOST_BASE_PATH", "value": path.HOST_BASE_PATH})
        env.append({"name": "CONTAINER_BASE_PATH", "value": path.CONTAINER_BASE_PATH})

        if json_pvc:
            self.getVolumeMounts().append(
                {"name": "jsoninput", "mountPath": "/jsoninput"}
            )
            self.getVolumes().append(
                {"name": "jsoninput", "configMap": {"name": json_pvc}}
            )

        if fileEnabled():
            self.getVolumeMounts().append(
                {"name": "transfer-volume", "mountPath": path.CONTAINER_BASE_PATH}
            )

            self.getVolumes().append(
                {
                    "name": "transfer-volume",
                    "persistentVolumeClaim": {"claimName": path.TRANSFER_PVC_NAME},
                }
            )

        self.add_s3_mount()

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

        Also passes `TESK_FTP_HOSTS`, the hosts that the filer may send them
        to, from the taskmaster's environment, if it is set there.

        Args:
            user: The FTP user name.
            pw: The FTP password.
        """
        env = self.getEnv()
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

        Args:
            pvc: The claim, with its volume mounts set.
        """
        self.getVolumeMounts().extend(pvc.volume_mounts)
        self.getVolumes().append(
            {"name": "task-volume", "persistentVolumeClaim": {"claimName": pvc.name}}
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

        self.spec["spec"]["template"]["metadata"]["name"] = self.name
        return self.spec
