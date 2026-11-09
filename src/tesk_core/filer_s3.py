"""Transfers over S3."""

import logging
import mimetypes
import os
import re
import sys
from typing import TYPE_CHECKING

import boto3
import botocore.exceptions

from tesk_core.transput import Transput, Type, within

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource


class S3Transput(Transput):
    """Transfer of an input or output over S3 (`s3://<bucket>/<key>`).

    The endpoint and credentials come from the AWS configuration (e.g.,
    `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE`).

    Attributes:
        bucket: Name of the bucket.
        file_path: Key of the object, or prefix of a directory.
        bucket_obj: The bucket, once the transfer is entered as a context.
    """

    def __init__(self, path: str, url: str, ftype: Type) -> None:
        """Describe the transfer.

        Args:
            path: Path in the filer's container.
            url: URL of the input or output.
            ftype: Whether it is a file or a directory.
        """
        Transput.__init__(self, path, url, ftype)
        self.bucket, self.file_path = self.get_bucket_name_and_file_path()
        self.bucket_obj: Bucket | None = None

    def __enter__(self) -> "S3Transput":
        """Connect to the bucket; exit with code 1 if it doesn't exist.

        Returns:
            The transfer itself.
        """
        client: S3ServiceResource = boto3.resource(
            "s3", endpoint_url=self.extract_endpoint()
        )
        if self.check_if_bucket_exists(client):
            sys.exit(1)
        self.bucket_obj = client.Bucket(self.bucket)
        return self

    def extract_endpoint(self) -> str:
        """Get the S3 endpoint of the AWS configuration.

        Returns:
            The endpoint URL.
        """
        return boto3.client("s3").meta.endpoint_url

    def check_if_bucket_exists(self, client: "S3ServiceResource") -> int:
        """Check that the bucket exists and can be accessed.

        Args:
            client: The S3 resource.

        Returns:
            0 if the bucket exists, otherwise 1.
        """
        try:
            client.meta.client.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            logging.error("Got status code: %s", e.response["Error"]["Code"])
            if e.response["Error"]["Code"] == "404":
                logging.error(
                    "Failed to fetch Bucket, reason: %s", e.response["Error"]["Message"]
                )
            return 1
        return 0

    def get_bucket_name_and_file_path(self) -> tuple[str, str]:
        """Get the bucket and the key from an URL like `s3://bucket/README.txt`.

        Returns:
            The bucket and the key.
        """
        bucket = self.netloc
        file_path = self.url_path[1:]

        return bucket, file_path

    def download_file(self) -> int:
        """Download an object to `path`, creating its directory.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            'Downloading s3 object: "%s" Target: %s',
            self.bucket + "/" + self.file_path,
            self.path,
        )
        basedir = os.path.dirname(self.path)
        os.makedirs(basedir, exist_ok=True)
        return self.get_s3_file(self.path, self.file_path)

    def get_content_type(self) -> str:
        """Guess the content type of `path` from its name.

        Returns:
            The content type, with `charset=utf-8` for text, or
            `application/octet-stream` if it can't be guessed.
        """
        mime, _ = mimetypes.guess_type(self.path)
        if mime is None:
            return "application/octet-stream"
        if mime.startswith("text/") or mime in (
            "application/json",
            "application/xml",
            "application/javascript",
        ):
            mime = f"{mime}; charset=utf-8"
        return mime

    def upload_file(self) -> int:
        """Upload `path` with its guessed content type.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            'Uploading s3 object: "%s" Target: %s',
            self.path,
            self.bucket + "/" + self.file_path,
        )
        content_type = self.get_content_type()
        logging.debug("Guessed Content-Type: %s for file: %s", content_type, self.path)
        try:
            # Pass ContentType via ExtraArgs so the object is uploaded with the
            # right MIME type. `bucket_obj` is set on entering the context
            self.bucket_obj.upload_file(  # type: ignore[union-attr]
                Filename=self.path,
                Key=self.file_path,
                ExtraArgs={"ContentType": content_type},
            )
        except (botocore.exceptions.ClientError, OSError) as err:
            logging.error(
                "File upload failed for '%s'", self.bucket + "/" + self.file_path
            )
            logging.error(err)
            return 1
        return 0

    def upload_dir(self) -> int:
        """Upload the files below `path`, recursively.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            'Uploading s3 object: "%s" Target: %s',
            self.path,
            self.bucket + "/" + self.file_path,
        )
        try:
            for item in os.listdir(self.path):
                path = os.path.join(self.path, item)
                if os.path.isdir(path):
                    file_type = Type.Directory
                elif os.path.isfile(path):
                    file_type = Type.File
                else:
                    # An exception is raised, if the object type is neither
                    # file or directory
                    logging.error("Object is neither file or directory : '%s' ", path)
                    raise OSError
                file_path = os.path.join(self.url, item)
                with S3Transput(path, file_path, file_type) as transfer:
                    if transfer.upload():
                        return 1
        except OSError as err:
            logging.error(
                "File upload failed for '%s'", self.bucket + "/" + self.file_path
            )
            logging.error(err)
            return 1
        return 0

    def download_dir(self) -> int:
        """Download the objects below the prefix to `path`.

        Objects whose local path would not be below `path` (e.g., keys
        with `..` segments) are skipped with a warning. Keys that end with
        `/` (folder markers) create their directory.

        Returns:
            0 on success, otherwise 1.
        """
        logging.debug(
            'Downloading s3 object: "%s" Target: %s',
            self.bucket + "/" + self.file_path,
            self.path,
        )
        client = boto3.client("s3", endpoint_url=self.extract_endpoint())
        if not self.file_path.endswith("/"):
            self.file_path += "/"
        objects = client.list_objects_v2(Bucket=self.bucket, Prefix=self.file_path)

        # If the file path does not exists in s3 bucket, 'Contents' key will
        # not be present in objects
        if "Contents" not in objects:
            logging.error("Got status code: %s", 404)
            logging.error("Invalid file path!.")
            return 1

        # Looping through the list of objects and downloading them
        for obj in objects["Contents"]:
            key = obj["Key"]
            file_name = os.path.basename(key)
            dir_name = os.path.dirname(key)
            prefix = re.escape(self.file_path.strip("/"))
            path_to_create = re.sub(r"^" + prefix, "", dir_name).strip("/")
            path_to_create = os.path.join(self.path, path_to_create)
            if file_name:
                target = os.path.join(path_to_create, file_name)
                inside = within(self.path, target)
            else:
                # A folder marker: only its directory, which may be `path`
                target = None
                inside = within(self.path, path_to_create) or (
                    os.path.realpath(path_to_create) == os.path.realpath(self.path)
                )
            if not inside:
                logging.warning("Skipping object %r outside the directory", key)
                continue
            os.makedirs(path_to_create, exist_ok=True)
            if target is not None and self.get_s3_file(target, key):
                return 1
        return 0

    def get_s3_file(self, file_name: str, key: str) -> int:
        """Download an object.

        Args:
            file_name: Path of the file to write.
            key: Key of the object.

        Returns:
            0 on success, otherwise 1.
        """
        try:
            # `bucket_obj` is set on entering the context
            self.bucket_obj.download_file(  # type: ignore[union-attr]
                Filename=file_name, Key=key
            )
        except botocore.exceptions.ClientError as err:
            logging.error("Got status code: %s", err.response["Error"]["Code"])
            logging.error(err.response["Error"]["Message"])
            return 1
        return 0
