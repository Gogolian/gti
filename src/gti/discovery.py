"""Bluetooth Classic device discovery.

``discover()`` returns paired/known devices and, where the platform allows it,
nearby devices found by an inquiry.  Discovery is best-effort: a platform that
cannot scan still returns its paired-device list.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .address import normalize_address

__all__ = ["Device", "discover", "pick_device", "format_devices"]

InputFn = Callable[[str], str]
PrintFn = Callable[..., object]

_BLUETOOTHCTL_DEVICE = re.compile(
    r"^Device\s+([0-9A-Fa-f:-]{17})\s+(.*)$"
)


@dataclass(frozen=True)
class Device:
    """A Bluetooth Classic device."""

    name: str
    address: str
    paired: bool = False


def _sort_key(device: Device) -> tuple:
    return (not device.paired, device.name.lower(), device.address)


def _merge(devices: Dict[str, Device], device: Device) -> None:
    existing = devices.get(device.address)
    if existing is None or (device.paired and not existing.paired):
        devices[device.address] = device
    elif not existing.name and device.name:
        devices[device.address] = Device(device.name, existing.address, existing.paired)


async def discover(timeout: float = 8.0) -> List[Device]:
    """Discover Bluetooth Classic devices without blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _discover_sync, timeout)


def _discover_sync(timeout: float) -> List[Device]:
    if sys.platform == "darwin":
        return _discover_macos(timeout)
    if sys.platform.startswith("linux"):
        return _discover_linux(timeout)
    if sys.platform == "win32":
        return _discover_windows(timeout)
    raise RuntimeError(f"device discovery is not supported on {sys.platform!r}")


def _discover_macos(timeout: float) -> List[Device]:
    import objc  # type: ignore

    ns: dict = {}
    objc.loadBundle(
        "IOBluetooth",
        bundle_path="/System/Library/Frameworks/IOBluetooth.framework",
        module_globals=ns,
    )
    IOBluetoothDevice = ns["IOBluetoothDevice"]
    devices: Dict[str, Device] = {}

    def _add(device: Any) -> None:
        try:
            address = normalize_address(str(device.addressString()))
            name = str(device.nameOrAddress() or "") or address
            paired = bool(device.isPaired())
        except Exception:
            return
        _merge(devices, Device(name=name, address=address, paired=paired))

    # Paired devices are always available and are the reliable path.  A
    # classic inquiry is intentionally not used here: IOBluetoothDeviceInquiry
    # is unstable from a non-GUI Python process on recent macOS releases.
    try:
        for device in IOBluetoothDevice.pairedDevices() or []:
            _add(device)
    except Exception:
        pass

    return sorted(devices.values(), key=_sort_key)


def _parse_bluetoothctl_devices(text: str) -> List[Device]:
    devices: Dict[str, Device] = {}
    for line in text.splitlines():
        match = _BLUETOOTHCTL_DEVICE.match(line.strip())
        if match is None:
            continue
        address = normalize_address(match.group(1))
        name = match.group(2).strip() or address
        devices[address] = Device(name=name, address=address)
    return sorted(devices.values(), key=_sort_key)


def _discover_linux(timeout: float) -> List[Device]:
    if shutil.which("bluetoothctl") is None:
        raise RuntimeError(
            "bluetoothctl not found; install BlueZ or pass --address explicitly"
        )
    # A scan populates bluetoothctl's cache; failures are non-fatal.
    try:
        subprocess.run(
            ["bluetoothctl", "--timeout", str(int(max(1, timeout))), "scan", "on"],
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )
    except Exception:
        pass
    try:
        result = subprocess.run(
            ["bluetoothctl", "devices"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:
        raise RuntimeError(f"failed to run bluetoothctl: {exc}") from exc
    return _parse_bluetoothctl_devices(result.stdout)


def _discover_windows(timeout: float) -> List[Device]:
    raise RuntimeError(
        "device discovery is not implemented on Windows yet; "
        "pair the device in Settings and pass --address explicitly"
    )


def format_devices(devices: List[Device]) -> List[str]:
    """Return one display line per device."""
    lines: List[str] = []
    for index, device in enumerate(devices, 1):
        tag = "paired" if device.paired else "found"
        lines.append(f"  {index}. {device.name}  [{device.address}] ({tag})")
    return lines


def pick_device(
    devices: List[Device],
    *,
    input_fn: InputFn = input,
    print_fn: PrintFn = print,
) -> Optional[Device]:
    """Interactively choose a device by number or address."""
    if not devices:
        print_fn("No Bluetooth devices found.")
        return None
    print_fn("Bluetooth devices:")
    for line in format_devices(devices):
        print_fn(line)
    while True:
        try:
            raw = input_fn("Select device number (or type an address, blank to cancel): ")
        except (EOFError, KeyboardInterrupt):
            return None
        raw = raw.strip()
        if not raw:
            return None
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(devices):
                return devices[index - 1]
            print_fn(f"gti: no device number {index}")
            continue
        try:
            address = normalize_address(raw)
        except ValueError as exc:
            print_fn(f"gti: {exc}")
            continue
        for device in devices:
            if device.address == address:
                return device
        return Device(name=address, address=address)
