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
first that fails. Without `-d`, the filer logs only errors; `-d` turns on
debug logging of the filer's own messages, while the HTTP and S3 libraries
(`urllib3`, `botocore`, `boto3`, `s3transfer`) still log only warnings and
errors. A missing `.netrc` file is logged at debug level only. The filer's
own log messages show URLs without their query strings (e.g. the parameters
of a presigned URL) and user info (`user:password@`). Response bodies of HTTP
errors are logged in the same way, with the query values and user info of
the request (and of redirects) and `.netrc` credentials redacted wherever
they appear (values of at least 4 characters), and cut after 1000
characters.

Each entry has a `path` in the container, a `type` (`FILE` or `DIRECTORY`)
and a `url`; an input can have `content` instead of a `url`, which the filer
writes to `path`. The scheme of the URL selects the protocol.

## Task paths in the filer

The filer's pod mounts the task's paths below `/tesk/task`, not at the
task's paths themselves: the input `/data/in.txt` is
`/tesk/task/data/in.txt` in the filer, and `/data/in.txt` in the
executors, which mount the same directory of the task's volume. Task
paths therefore can't overlay the filer's own files (e.g. its program) or
its configuration (`/aws`, `/opt/home/.netrc`, `/jsoninput`, the shared
volume). Paths are normalized first, for the filer and the executors'
mounts: `..` and `.` components are resolved (`..` as far as the root, so
`/../etc` is `/tesk/task/etc`), and repeated and trailing slashes dropped,
so `/out/` and `/out` share one mount. Glob patterns of `file:` outputs
are normalized the same way, and expanded in the filer only. Log messages
of the transfers show the paths in the filer, the debug messages
"Processing file" and "Processed file" the task's. The filer pod doesn't
mount a service account token, and the image's entrypoint is the absolute
path `/usr/bin/filer`.

The taskmaster and the filer must be of the same version: with an older
taskmaster, a newer filer looks for the task's paths below `/tesk/task`,
where nothing is mounted, and the other way round. Inputs then don't reach
the executors and outputs aren't found, so tasks fail.

## Outputs with wildcards

An output whose `path` has wildcards (`*`, `?`, `[`; POSIX pattern
matching, as in TES 1.1) and a `path_prefix` is expanded in the filer, for
every scheme. Each match is uploaded to the output's `url`, a directory,
plus its path below `path_prefix`: with `path` `/data/*/out.txt`,
`path_prefix` `/data` and `url` `s3://bucket/run`, `/data/s1/out.txt` is
uploaded to `s3://bucket/run/s1/out.txt`. Matches that are directories are
uploaded with their contents. As in the shell, `*` doesn't match names
that start with `.`, while `.command.*` does. A pattern that matches
nothing uploads nothing and succeeds, so a single-file class such as
`out.tx[*t]` makes an optional output. A match that isn't below
`path_prefix` fails the output. The executors and the filer mount the
pattern's deepest directory without wildcards (`/data`).

Outputs with wildcards but without `path_prefix` (TES 1.0) are taken as
before: `file:` outputs are expanded into the directory of the URL, and
for other schemes the path is a file name.

## Symbolic links in outputs

Outputs can't be or contain symbolic links. The filer fails an output, with
an error that names the link, if any component of its path is a link (e.g.
`/data/out` for the output `/data/out/result.txt`), or, for a directory, if
any file or directory below it is one (output paths are normalized first,
see [Task paths in the filer](#task-paths-in-the-filer)). For outputs with
wildcards, a link in any match fails the output. Links are not followed, as the filer would resolve
them in its own container rather than the executor's; nothing of such an
output is uploaded. Executors that create links in
their outputs should copy the files they point to instead (e.g. `cp -L`).
The outputs filer mounts the task's paths read-only.

## Protocols

| Scheme | Inputs | Outputs |
|---|---|---|
| `http`, `https` | Files (`GET`); not directories | Files (`PUT`), and directories file by file |
| `ftp` | Files and directories, including empty ones | Files and directories, including empty ones (creating the remote directories) |
| `s3` | Files and directories | Files and directories |
| `file` (or no scheme) | Files and directories | Files and directories (globs in file paths, also without `path_prefix`) |

### HTTP(S)

Inputs are downloaded with `GET`, streamed to disk in chunks of 1 MiB, so
that large files don't need memory of their size. A connection that breaks
off during the body, or a failed write (e.g. a full volume), fails the
input, and its partial file is removed.

Outputs are uploaded with `PUT`, as bytes, streamed from disk: a directory
is uploaded file by file (stopping at the first that fails), each to the
directory's URL plus the escaped file name (the query of the URL, e.g. a
token of a presigned URL, is kept).

Any status other than 2xx fails the transfer, as does a request error (e.g.
a failed connection or a timeout), which is logged with its type. Of the
body of an error response, only the start is read for the log.

Requests time out after `FILER_HTTP_TIMEOUT` seconds (default 300) without
progress: when connecting, and between the parts of a response or upload,
not for the whole transfer, so large files work. The taskmaster passes the
variable on from its own environment; an empty value counts as unset, and an
invalid one (not a positive number of at most 1000000) is logged once and
replaced by the default. Credentials can come from a `.netrc` file (see
[Credentials from `.netrc`](#credentials-from-netrc)), which `requests`
reads for the URL's host.

### FTP

The filer connects to the URL's host and port (21 if the URL has none);
user information in the URL is ignored. A URL without a valid host or port
(e.g. `ftp://ftp.example.org:99999/data`) fails the transfer with an error.

Each input or output opens one connection and logs in once; a directory's
files and subdirectories, downloaded or uploaded, share it. A failed
transfer fails the filer's job, which Kubernetes retries up to the filer's
`backoffLimit` (`FILER_BACKOFF_LIMIT`; see
[Taskmaster](taskmaster.md#environment-variables)).

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

Directory inputs download every object below the URL's key prefix (all
pages of the listing, also beyond 1000 objects), keeping the paths below
it. Objects whose local path would not be below the input's directory (e.g.
keys with `..` segments) are skipped with a warning; keys that end with `/`
(folder markers) create their directory.

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
| `CONTAINER_BASE_PATH` | Path where the filer mounts the shared volume; neither in nor around `/tesk/task` (the taskmaster fails with an error otherwise). |
| `TRANSFER_PVC_NAME` | Name of the PVC of the shared volume. |

A URL `file://<HOST_BASE_PATH>/a/b` refers to `<CONTAINER_BASE_PATH>/a/b`
in the filer: with `HOST_BASE_PATH=/tmp` and `CONTAINER_BASE_PATH=/transfer`,
`file:///tmp/a/b` is `/transfer/a/b`. TESK's Helm chart sets these with `transfer.active` (see
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation).

Directories are copied recursively. A directory copied to one that exists
is merged into it: subdirectories that exist on both sides are merged, and
files that exist on both sides replaced. Directories that exist keep their
own mode (their metadata isn't copied). Links in the existing directory,
and the destination itself if it is a link, are replaced by the copied file
or directory, not followed. A file that meets a directory of its name fails
the copy.

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
