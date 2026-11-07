# Upgrading

Changes that operators must act on when they upgrade the taskmaster and the
filer. Upgrade both images together: the taskmaster passes the settings on
to the filer.

## Next release (after 0.10.6)

### FTP credentials go only to configured hosts

The filer sends the FTP credentials of `TESK_FTP_USERNAME` and
`TESK_FTP_PASSWORD` only to the hosts in the new setting `TESK_FTP_HOSTS`
(see [Filer](filer.md#ftp)). Without it, it logs in to every FTP server
anonymously, and logs an error that says so.

To keep using these credentials, do one of the following before you
upgrade:

- Set `TESK_FTP_HOSTS` in the taskmaster's environment to the hosts of the
  FTP account, e.g. `ftp.example.org`. With TESK's Helm chart, use the
  chart's value for it (see
  [Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in
  the TESK documentation).
- Or put the credentials into a `.netrc` file, with one entry per host (see
  [Credentials from `.netrc`](filer.md#credentials-from-netrc)).

Deployments without these credentials, or with a `.netrc` file, need no
change.
