# Filer

The filer stages the files of a task. The [taskmaster](taskmaster.md) runs it
twice per task, as a Kubernetes job with the task's volume mounted: with
`inputs` before the executors, to download the inputs onto the volume, and
with `outputs` after them, to upload the outputs.

```text
filer [-d] {inputs|outputs} DATA
```

`DATA` is the task as JSON, or a gzipped JSON file if it ends with `.gz` (the
taskmaster passes `/jsoninput/JSON_INPUT.gz`, from a ConfigMap, or the JSON
in the environment variable `JSON_INPUT`). The filer processes the entries of
`inputs` or `outputs` one after another, and stops with exit code 1 at the
first that fails. `-d` turns on debug logging. The filer's own log messages
show URLs without their query strings (e.g. the parameters of a presigned
URL) and user info (`user:password@`); response bodies logged on errors are
shown as received.

Each entry has a `path` in the container, a `type` (`FILE` or `DIRECTORY`)
and a `url`; an input can have `content` instead of a `url`, which the filer
writes to `path`. The scheme of the URL selects the protocol.

## Protocols

| Scheme | Inputs | Outputs |
|---|---|---|
| `http`, `https` | Files (`GET`); not directories | Files (`PUT`), and directories file by file |
| `ftp` | Files and directories (not empty ones) | Files and directories (not empty ones; creating the remote directories) |
| `s3` | Files and directories | Files and directories |
| `file` (or no scheme) | Files and directories | Files and directories (globs in file paths) |

### HTTP(S)

Inputs are downloaded with `GET`. Outputs are uploaded with `PUT`, as bytes,
streamed from disk: a directory is uploaded file by file (stopping at the
first that fails), each to the directory's URL plus the escaped file name
(the query of the URL, e.g. a token of a presigned URL, is kept). Any status other than 2xx fails the
transfer, as does a request error (e.g. a failed connection), which is logged
with its type. Credentials can come from a `.netrc` file (see
[Credentials from `.netrc`](#credentials-from-netrc)), which `requests`
reads for the URL's host.

### FTP

The filer logs in with the first credentials it finds:

1. The entry in a mounted `.netrc` file for the URL's host and port (e.g.
   `machine ftp.example.org:2121`) or for its host (`machine
   ftp.example.org`), matched as written, including case; otherwise its
   `default` entry.
2. `TESK_FTP_USERNAME` and `TESK_FTP_PASSWORD`, if both are set.
3. Anonymously.

If the server refuses the credentials, the filer logs in anonymously. TESK's
Helm chart sets the two variables from a Kubernetes secret
(`ftp.classic_ftp_secret`; see
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation).

### S3

`s3://<bucket>/<key>` URLs are transferred with boto3, which reads its
endpoint and credentials from `/aws/config` and `/aws/credentials`
(`AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`). The taskmaster mounts
them from the keys `config` and `credentials` of the Kubernetes secret
`aws-secret`, if it exists. For example, for an S3-compatible store:

```ini title="config"
[default]
endpoint_url = https://s3.example.org
```

```ini title="credentials"
[default]
aws_access_key_id = <key id>
aws_secret_access_key = <secret>
```

The bucket must exist. Outputs are uploaded with a `Content-Type` guessed
from the file name. Checksums are calculated and validated only where the
S3 API requires them (`AWS_REQUEST_CHECKSUM_CALCULATION` and
`AWS_RESPONSE_CHECKSUM_VALIDATION` set to `WHEN_REQUIRED`), for stores that
don't support the newer checksums.

### Shared volume (`file:`)

With `file:` URLs, the filer copies files between the task's volume and a
volume shared with the nodes or other services, e.g. a PVC that a workflow
engine also mounts. The protocol is off unless the taskmaster has these
environment variables, which it passes on to the filer:

| Variable | Description |
|---|---|
| `HOST_BASE_PATH` | Absolute base path of the shared volume in the URLs. The path of every `file:` URL must be absolute and, after resolving `..` segments, be this path or below it, e.g. `/tmp/a` for `/tmp`, but not `/tmp2/a` or `/tmp/../a`. |
| `CONTAINER_BASE_PATH` | Path where the filer mounts the shared volume. |
| `TRANSFER_PVC_NAME` | Name of the PVC of the shared volume. |

A URL `file://<HOST_BASE_PATH>/a/b` refers to `<CONTAINER_BASE_PATH>/a/b`
in the filer: with `HOST_BASE_PATH=/tmp` and `CONTAINER_BASE_PATH=/transfer`,
`file:///tmp/a/b` is `/transfer/a/b`. TESK's Helm chart sets these with `transfer.active` (see
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation).

## Credentials from `.netrc`

If the taskmaster has the environment variable `NETRC_SECRET_NAME`, it mounts
the key `.netrc` of that Kubernetes secret into the filer as
`/opt/home/.netrc`, and sets `HOME` to `/opt/home`. The filer then uses it
for FTP and HTTP(S), as described above. TESK's Helm chart creates such a
secret with `ftp.netrc_secret`. A `.netrc` file has one entry per host:

```text
machine ftp.example.org
login <user>
password <password>
```
