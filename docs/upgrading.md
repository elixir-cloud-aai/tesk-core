# Upgrading

Changes that operators must act on when they upgrade the taskmaster and the
filer. Upgrade both images together: the taskmaster passes the settings on
to the filer.

## Unreleased

### FTP credentials go only to configured hosts

The filer sends the FTP credentials of `TESK_FTP_USERNAME` and
`TESK_FTP_PASSWORD` only to the hosts in the new setting `TESK_FTP_HOSTS`
(see [Filer](filer.md#ftp)). Without it, it logs in to every FTP server
anonymously, and logs an error that says so.

To keep using these credentials, do one of the following before you
upgrade:

- Set `TESK_FTP_HOSTS` in the taskmaster's environment to the hosts of the
  FTP account, e.g. `ftp.example.org`. With TESK's Helm chart, set
  `ftp.hosts` (see
  [Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in
  the TESK documentation).
- Or put the credentials into a `.netrc` file, with one entry per host (see
  [Credentials from `.netrc`](filer.md#credentials-from-netrc)).

Deployments without these credentials, or with a `.netrc` file, need no
change.

### `.netrc` and the env credentials together

With a `.netrc` file, the filer used to ignore `TESK_FTP_USERNAME` and
`TESK_FTP_PASSWORD`, and logged in anonymously to hosts without an entry.
Now it uses them for hosts without an entry that are in `TESK_FTP_HOSTS`
(see [Filer](filer.md#ftp)). Entries of the `.netrc` file still win: one
for the host and port or for the host, and also a `default` entry, which
applies to every host. No change is needed, unless you listed hosts in
`TESK_FTP_HOSTS` that must still be accessed anonymously.

If the env credentials are set as well, but `TESK_FTP_HOSTS` is not, the
filer now logs an error about `TESK_FTP_HOSTS` when it logs in to a host
without an entry. Set `TESK_FTP_HOSTS`, or remove the env credentials if
the `.netrc` file has all you need. An empty `TESK_FTP_USERNAME` counts as
no credentials, with or without `.netrc`.
