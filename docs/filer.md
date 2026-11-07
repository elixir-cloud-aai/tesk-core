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
URL) and user info (`user:password@`). Response bodies of HTTP errors are
logged in the same way, with the query values and user info of the request
redacted wherever they appear, and cut after 1000 characters.

Each entry has a `path` in the container, a `type` (`FILE` or `DIRECTORY`)
and a `url`; an input can have `content` instead of a `url`, which the filer
writes to `path`. The scheme of the URL selects the protocol.

## Protocols

| Scheme | Inputs | Outputs |
|---|---|---|
| `http`, `https` | Files (`GET`); not directories | Files (`PUT`), and directories file by file |
| `ftp` | Files and directories, including empty ones | Files and directories, including empty ones (creating the remote directories) |
| `s3` | Files and directories | Files and directories |
| `file` (or no scheme) | Files and directories | Files and directories (globs in file paths) |

### HTTP(S)

Inputs are downloaded with `GET`. Outputs are uploaded with `PUT`, as bytes,
streamed from disk: a directory is uploaded file by file (stopping at the
first that fails), each to the directory's URL plus the escaped file name
(the query of the URL, e.g. a token of a presigned URL, is kept). Any status
other than 2xx fails the transfer, as does a request error (e.g. a failed
connection or a timeout), which is logged with its type.

Requests time out after `FILER_HTTP_TIMEOUT` seconds (default 300) without
progress: when connecting, and between the parts of a response or upload,
not for the whole transfer, so large files work. The taskmaster passes the
variable on from its own environment; an empty value counts as unset, and an
invalid one (not a positive number) is logged once and replaced by the
default. Credentials can come from a `.netrc` file (see
[Credentials from `.netrc`](#credentials-from-netrc)), which `requests`
reads for the URL's host.

### FTP

The filer connects to the URL's host and port (21 if the URL has none);
user information in the URL is ignored. A URL without a valid host or port
(e.g. `ftp://ftp.example.org:99999/data`) fails the transfer with an error.

Directory downloads skip, with a warning, listing entries whose names aren't
a single path component (empty, `.`, `..`, or with `/` or `\`).

The filer logs in with the first credentials it finds:

1. The entry in a mounted `.netrc` file for the URL's host and port as
   written (e.g. `machine ftp.example.org:2121` for
   `ftp://ftp.example.org:2121/data`); for port 21 (or none), the entry for
   the host with `:21` or without a port (`machine ftp.example.org`);
   otherwise its `default` entry, if it has one, for any host and port.
   Hosts are matched as written, including case.
2. `TESK_FTP_USERNAME` and `TESK_FTP_PASSWORD`, if both are set (the user
   name not empty) and the URL's host is in `TESK_FTP_HOSTS` (see below).
3. Anonymously.

If the server refuses the credentials, the filer logs in anonymously.
TESK's Helm chart sets the credentials from a Kubernetes secret
(`ftp.classicFtpSecret`) and `TESK_FTP_HOSTS` from `ftp.hosts` (see
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation).

`TESK_FTP_HOSTS` lists the hosts that the credentials are sent to,
separated by commas, e.g.
`ftp.example.org,data.example.org:2121,[2001:db8::1]`:

- Each entry is `host` or `host:port`; IPv6 addresses go in brackets.
  Spaces around entries are ignored.
- Hosts match exactly, ignoring case: `ftp.example.org` matches neither
  `example.org` nor `ftp.example.org.other.org`. There are no wildcards.
- An entry without a port matches the host on port 21 only (and URLs
  without a port); list other ports explicitly, e.g.
  `data.example.org:2121`.
- Entries with user information, a path, whitespace or control characters
  inside, or an empty, invalid or zero port (e.g. `ftp.example.org:`)
  match nothing;
  the filer logs an error, once per run, with their number (not their
  text).

If `TESK_FTP_USERNAME` and `TESK_FTP_PASSWORD` are set, but
`TESK_FTP_HOSTS` is not (or has no entries), the credentials are sent to no
host: the filer logs in anonymously and logs an error, once per run, that
says to set `TESK_FTP_HOSTS` or to use a `.netrc` file. Log messages contain
neither the credentials nor the user information of URLs.

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

Only `s3://` URLs use S3. The bucket is the URL's host part, taken as
written, so bucket names may contain `s3` (e.g. `s3://mys3data/out.txt`).
`http(s)://` URLs are transferred over HTTP, even if they point to an S3
endpoint (e.g. `https://mybucket.s3.amazonaws.com/file.txt`); to use S3 for
them, rewrite them as `s3://mybucket/file.txt`.

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
secret with `ftp.netrcSecret`. A `.netrc` file has one entry per host, or
per host and port:

```text
machine ftp.example.org
login <user>
password <password>

machine data.example.org:2121
login <user>
password <password>
```

For FTP, an entry without a port is used for port 21 only (see
[FTP](#ftp)); a `default` entry is used for every host and port without an
entry of its own.
