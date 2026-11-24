# Jeevo connector

The connector is a small program you run on your own server to store and
stream media for [Jeevo](https://jeevo.net). Your communities' videos, photos
and GIFs live on your server and are streamed from it to viewers; Jeevo never
stores them.

A connector lets you create communities on Jeevo. One connector can hold as
many communities as you like, and you can let other people create communities
on yours.

## Contents

- [Requirements](#requirements)
- [Install](#install)
- [Network](#network)
- [Settings](#settings)
- [Updating](#updating)
- [Moving or reinstalling](#moving-or-reinstalling)
- [Uninstalling](#uninstalling)
- [Privacy and security](#privacy-and-security)
- [Troubleshooting](#troubleshooting)

## Requirements

- Linux with systemd, x86_64 or ARM64 (Debian, Ubuntu, Fedora, Arch, openSUSE
  or Alpine based). No Windows or macOS build.
- Root access (`sudo`) to install.
- An inbound TCP port (28443 by default) for the best quality. Without one,
  media is relayed through Jeevo at up to 720p.
- Optional: an NVIDIA or Intel GPU for faster video processing.

### Recommended specs

|         | Minimum                 | Recommended                      |
| ------- | ----------------------- | -------------------------------- |
| CPU     | 2 cores                 | 4+ cores, or a GPU encoder       |
| Memory  | 2 GB                    | 4 GB                             |
| Disk    | 2 GB (excluding media)  | An SSD or large HDD for media    |
| Network | Any internet connection | 50 Mbps+ upload and an open port |

Streaming copies take roughly 60–80% of the originals' size on top.

## Install

1. In Jeevo, open **Settings → Connectors → Add connector**, name it and press
   **Generate license key**. The key works once and expires after a day.
2. Run the install command Jeevo shows you on your server:
3. Choose the install directory (default `/opt/jeevo-connector`) and the data
   directory for your media (default `/var/lib/jeevo-connector`, put it on your
   big disk).
4. Paste your license key.

The installer installs ffmpeg and Python, verifies the connector's signature
before running it, connects it to Jeevo and starts it as a systemd service.
Within a minute your connector gets its own domain and HTTPS certificate, and
shows as **Online** in Jeevo. Create a community and pick it as the place to
store media.

To check it at any time: `sudo jeevo-connector doctor`.

## Network

Viewers stream straight from your connector over HTTPS, so its port (TCP
28443 by default) must be reachable from the internet.

- **VPS:** usually works as is. If you run a firewall, allow the port (the
  installer offers to for `ufw` and `firewalld`).
- **Home server:** forward the port on your router. If the outside port is
  different, set it as the _Public port_ in Jeevo.

Your public IP is detected automatically and your connector's domain follows
it. If viewers can't reach the connector, it switches to **Relayed through
Jeevo**: everything still works, but video is limited to 720p. Jeevo shows
which mode you're in and why.

## Settings

Everything is configured in Jeevo under **Settings → Connectors → your
connector**. Changes apply within seconds, without a restart.

## Updating

Press **Update now** in Jeevo, run `sudo jeevo-connector update`, or turn on
automatic updates in Jeevo. Every update is signature-checked, and if a new
version fails to start the connector rolls back by itself.

## Moving or reinstalling

1. Copy the data directory to the new machine.
2. In Jeevo, generate a new license key from the connector's **Install**
   settings.
3. Run the install command on the new machine, choose the copied data
   directory and paste the new key.

To reinstall on the same machine, delete `/etc/jeevo-connector/connector.json`
first; your media is kept. The connector keeps its domain and communities, and
the old install is disconnected as soon as the new one connects.

## Uninstalling

```bash
sudo systemctl disable --now jeevo-connector
sudo rm -rf /opt/jeevo-connector /etc/jeevo-connector /usr/local/bin/jeevo-connector \
            /etc/systemd/system/jeevo-connector.service
sudo systemctl daemon-reload
sudo userdel jeevo-connector
# Only if you also want to delete your media:
sudo rm -rf /var/lib/jeevo-connector
```

Then delete the connector in Jeevo, and close the port if you opened it.

## Privacy and security

- **Jeevo never receives your media.** It knows your connector's name,
  version, hostname, public IP, disk usage and processing status.
- **Nothing is public.** Every upload and every view needs a short-lived token
  Jeevo issued for that exact item. Tokens never say who is watching.
- **Your IP:** in direct mode viewers can see your server's public IP. Choose
  **Relayed** under Network in Jeevo to hide it.
- **Your keys stay yours.** The HTTPS private key is created on your server
  and never leaves it.
- **The service is locked down.** It runs as its own unprivileged user, can
  only write to its own directories and is sandboxed by systemd.
- **Outside services:** the connector asks public "what is my IP" services for
  your address, and downloads an AI model (about 23 MB) from GitHub on first
  start, checked against a fixed checksum.
- **If your server is compromised,** revoke the connector in Jeevo. Its
  credentials stop working immediately.

To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Troubleshooting

Start with `sudo jeevo-connector doctor` and the log
(`sudo journalctl -u jeevo-connector -f`).

| Problem                                  | What to do                                                                 |
| ---------------------------------------- | -------------------------------------------------------------------------- |
| Install says _signature does not verify_ | Stop. Make sure you used the exact command Jeevo showed you.               |
| _License key has expired_                | Generate a new one in Jeevo.                                               |
| Connector shows **Offline**              | Check the service is running and the server can reach Jeevo.               |
| Stuck on **Relayed through Jeevo**       | Jeevo shows the reason. Usually the port isn't forwarded or is firewalled. |
| Certificate error                        | Usually temporary. Check the system clock is correct.                      |
| Uploads fail with _out of space_         | Free up space. The connector keeps 2 GB free                               |
| Videos stay in _Processing_              | Check the AI worker status in Jeevo, then the log.                         |
| Processing is slow                       | Choose a GPU encoder in Jeevo or use fewer qualities.                      |
