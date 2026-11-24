# Security policy

The connector runs on other people's servers, holds their keys and serves
their media to the internet, so security reports are very welcome.

## Reporting a vulnerability

**Please do not open a public issue, discussion or pull request.**

Report it privately through GitHub instead: open the
[Security tab](../../security) of this repository and choose
**Report a vulnerability**. Only the maintainer can see the report.

Please include:

- the connector version (`jeevo-connector version`) and operating system;
- the steps to reproduce it, or a proof of concept;
- what an attacker could do with it, and what they would need first
  (network access, a valid token, a local account, ...).

## Supported versions

Security fixes go into the **latest stable release** and the **latest beta
release** (`vX.Y.Z-rc.N`). Older releases are not supported: update to get a
fix.

## Scope

In scope: problems in this repository, for example:

- reading or changing media without a valid playback or upload token, or with
  a token issued for a different item or connector;
- getting around upload size limits or quotas;
- path traversal, or reading or writing files outside the connector's
  directories;
- running code through a crafted upload (the media pipeline or the AI worker);
- installing an update that was not signed with the release key, or rolling
  a connector back to an older release;
- gaining root or another user's privileges through the installer or the
  service;
- leaking the connector's credentials, keys or the operator's private
  details.

Out of scope:

- the Jeevo server, website and apps (they are not in this repository; report
  those through the same form and they will be passed on);
- attacks that need root or the service account on the connector's server
  already;
- denial of service by someone the operator has given upload access to, or by
  plain traffic volume;
- vulnerabilities in ffmpeg, Python packages or other dependencies themselves:
  report those to their projects, unless the connector makes them reachable
  or worse;
- things the README already describes as visible, such as a direct-mode
  connector's public IP address.

## Disclosure

Please keep the details private until a fix is released. Fixes are announced
in a GitHub security advisory, which credits the reporter unless they would
rather not be named. There is no bug bounty.
