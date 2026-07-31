"""Where a client should actually dial.

The address a server *listens* on and the address a client can *reach* are not
the same string, and two of the differences bite often enough to belong in the
binder rather than in every caller:

  - ``0.0.0.0`` (and ``::``) is a listen address meaning "every interface". As a
    destination it is not routable on every platform; the connectable spelling
    of the same server is ``127.0.0.1``.
  - Under WSL, a server running on the Windows host is not on the WSL loopback.
    The host's address is the nameserver in ``/etc/resolv.conf``, so a
    configured ``127.0.0.1`` is worth retrying against it before declaring the
    server unreachable.

Both are resolved into an ordered list of candidates, tried in order on
connect. Nothing here performs I/O beyond reading two well-known files once per
process.
"""

from __future__ import annotations

import ipaddress
import os
from pathlib import Path

__all__ = [
    "candidate_hosts",
    "is_loopback_host",
    "is_unspecified_host",
    "running_in_wsl",
    "wsl_host_ip",
]


def _detect_wsl() -> bool:
    if os.environ.get("WSL_DISTRO_NAME") or os.environ.get("WSL_INTEROP"):
        return True
    for probe in ("/proc/sys/kernel/osrelease", "/proc/version"):
        try:
            contents = Path(probe).read_text(encoding="utf-8").lower()
        except OSError:
            continue
        if "microsoft" in contents or "wsl" in contents:
            return True
    return False


def _detect_wsl_host_ip() -> str | None:
    resolv = Path("/etc/resolv.conf")
    if not resolv.exists():
        return None
    try:
        for raw_line in resolv.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or not line.startswith("nameserver"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            candidate = parts[1]
            try:
                address = ipaddress.ip_address(candidate)
            except ValueError:
                continue
            if address.version == 4:
                return candidate
    except OSError:
        return None
    return None


_RUNNING_IN_WSL = _detect_wsl()
_WSL_HOST_IP = _detect_wsl_host_ip() if _RUNNING_IN_WSL else None


def running_in_wsl() -> bool:
    return _RUNNING_IN_WSL


def wsl_host_ip() -> str | None:
    return _WSL_HOST_IP


def is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower()
    if normalized in {"localhost", "loopback"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def is_unspecified_host(host: str) -> bool:
    normalized = host.strip().lower()
    if normalized in {"0.0.0.0", "::", "::0"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_unspecified
    except ValueError:
        return False


def candidate_hosts(host: str) -> list[str]:
    """The ordered destinations to try for a configured ``host``."""
    host = (host or "").strip()
    hosts: list[str] = []
    if not host:
        hosts.append("127.0.0.1")
    elif is_unspecified_host(host):
        hosts.append("127.0.0.1")
    else:
        hosts.append(host)
    if _RUNNING_IN_WSL and _WSL_HOST_IP and (not host or is_unspecified_host(host) or is_loopback_host(host)):
        if _WSL_HOST_IP not in hosts:
            hosts.append(_WSL_HOST_IP)
    return hosts
