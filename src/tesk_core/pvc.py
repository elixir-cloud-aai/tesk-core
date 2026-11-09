"""The persistent volume claim of a task."""

import logging
import os
from typing import Any

from kubernetes import client
from kubernetes.client.rest import ApiException

from tesk_core.Util import pprint


class PVC:
    """The persistent volume claim of a task, shared by its jobs.

    Attributes:
        name: Name of the claim.
        spec: The manifest of the claim.
        subpath_idx: Index of the next subpath (`get_subpath`).
        namespace: Namespace of the claim.
        cv1: Client of the Kubernetes core API.
        volume_mounts: Volume mounts of the task's paths on the claim
            (`set_volume_mounts`).
    """

    volume_mounts: list[dict[str, Any]]

    def __init__(
        self, name: str = "task-pvc", size_gb: int = 1, namespace: str = "default"
    ) -> None:
        """Describe the claim; `create` creates it.

        The environment variable `STORAGE_CLASS_NAME` sets a storage class
        other than the cluster's default.

        Args:
            name: Name of the claim.
            size_gb: Requested storage, in GiB.
            namespace: Namespace of the claim.
        """
        self.name = name
        self.spec: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": str(size_gb) + "Gi"}},
            },
        }

        self.subpath_idx = 0
        self.namespace = namespace
        self.cv1 = client.CoreV1Api()

        # The environment variable 'TESK_API_TASKMASTER_ENVIRONMENT_STORAGE_CLASS_NAME'
        # can be set to the preferred, non-default, user-defined storageClass
        if os.environ.get("STORAGE_CLASS_NAME") is not None:
            self.spec["spec"].update(
                {"storageClassName": os.environ.get("STORAGE_CLASS_NAME")}
            )

    def set_volume_mounts(self, mounts: list[dict[str, Any]]) -> None:
        """Set the volume mounts of the task's paths on the claim.

        Args:
            mounts: The volume mounts.
        """
        self.volume_mounts = mounts

    def get_subpath(self) -> str:
        """Get a new subpath of the claim.

        Returns:
            The subpath, `dir<n>`, with `n` counting up from 0.
        """
        subpath = "dir" + str(self.subpath_idx)
        self.subpath_idx += 1
        return subpath

    def create(self) -> Any:
        """Create the claim, or read it if it exists.

        Returns:
            The claim, as returned by the Kubernetes API.

        Raises:
            ApiException: The Kubernetes API returned an error other than a
                conflict.
        """
        logging.debug("Creating PVC...")
        logging.debug(pprint(self.spec))
        try:
            return self.cv1.create_namespaced_persistent_volume_claim(
                self.namespace, self.spec
            )
        except ApiException as ex:
            if ex.status == 409:
                logging.debug("Reading existing PVC: %s", self.name)
                return self.cv1.read_namespaced_persistent_volume_claim(
                    self.name, self.namespace
                )
            # Without the response body, which can repeat the request
            logging.debug(
                "Can't create PVC '%s': %s %s", self.name, ex.status, ex.reason
            )
            raise ApiException(ex.status, ex.reason) from ex

    def delete(self, request_timeout: tuple[float, float] | None = None) -> None:
        """Delete the claim.

        Args:
            request_timeout: Seconds to connect to and to wait for the
                Kubernetes API, per attempt; none waits as long as it takes.
        """
        cv1 = client.CoreV1Api()
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs["_request_timeout"] = request_timeout
        cv1.delete_namespaced_persistent_volume_claim(
            self.name, self.namespace, body=client.V1DeleteOptions(), **kwargs
        )
