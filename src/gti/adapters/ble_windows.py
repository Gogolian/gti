"""Windows BLE GATT transport (WinRT).

This module is imported only on Windows.  It uses the in-box WinRT GATT
server/client APIs (``GattServiceProvider`` / ``BluetoothLEDevice``) and runs
an asyncio loop in a child process, mirroring the macOS BLE helper.

The Windows BLE peripheral role requires Windows 10 1803+ and a Bluetooth
adapter that supports the LE peripheral role.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from multiprocessing.connection import Connection
from typing import Any, List, Optional

from ..discovery import Device
from .ble_framing import FrameAssembler, fragment_frame

__all__ = ["ble_process_windows", "ble_scan_process_windows"]

logger = logging.getLogger(__name__)

BLE_SERVICE_UUID = "e6031152-a17a-4f76-9aa5-cba1b4321dcf"
BLE_RX_UUID = "e6031152-a17a-4f76-9aa5-cba1b4321dc1"
BLE_TX_UUID = "e6031152-a17a-4f76-9aa5-cba1b4321dc2"

_DEFAULT_PAYLOAD = 180


def _send_control(conn: Connection, **payload: Any) -> None:
    conn.send_bytes(b"C" + json.dumps(payload).encode())


def _address_to_mac(address: int) -> str:
    return ":".join(
        f"{(address >> (8 * i)) & 0xFF:02X}" for i in reversed(range(6))
    )


def _buffer_from_bytes(data: bytes) -> Any:
    from winrt.windows.storage.streams import DataWriter

    writer = DataWriter()
    writer.write_bytes(data)
    return writer.detach_buffer()


def _bytes_from_buffer(buffer: Any) -> bytes:
    from winrt.windows.storage.streams import DataReader

    if buffer is None:
        return b""
    reader = DataReader.from_buffer(buffer)
    data = bytearray(int(buffer.length))
    reader.read_bytes(data)
    return bytes(data)


async def _pipe_reader(conn: Connection, on_frame: Any) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            frame = await loop.run_in_executor(None, conn.recv_bytes)
        except (EOFError, OSError):
            return
        try:
            await on_frame(frame)
        except Exception:
            logger.exception("BLE write failed")
            return


async def _run_peripheral(conn: Connection) -> None:
    from winrt.windows.devices.bluetooth.genericattributeprofile import (
        GattCharacteristicProperties,
        GattLocalCharacteristicParameters,
        GattProtectionLevel,
        GattServiceProvider,
        GattServiceProviderAdvertisingParameters,
        GattWriteOption,
    )

    loop = asyncio.get_running_loop()
    service_uuid = uuid.UUID(BLE_SERVICE_UUID)
    rx_uuid = uuid.UUID(BLE_RX_UUID)
    tx_uuid = uuid.UUID(BLE_TX_UUID)

    result = await GattServiceProvider.create_async(service_uuid)
    if result.error != 0:
        _send_control(conn, type="error", message=f"GattServiceProvider error {result.error}")
        return
    provider = result.service_provider

    rx_params = GattLocalCharacteristicParameters()
    rx_params.characteristic_properties = (
        GattCharacteristicProperties.WRITE
        | GattCharacteristicProperties.WRITE_WITHOUT_RESPONSE
    )
    rx_params.write_protection_level = GattProtectionLevel.PLAIN
    rx_result = await provider.service.create_characteristic_async(rx_uuid, rx_params)
    if rx_result.error != 0:
        _send_control(conn, type="error", message=f"RX characteristic error {rx_result.error}")
        return
    rx = rx_result.characteristic

    tx_params = GattLocalCharacteristicParameters()
    tx_params.characteristic_properties = GattCharacteristicProperties.NOTIFY
    tx_params.read_protection_level = GattProtectionLevel.PLAIN
    tx_result = await provider.service.create_characteristic_async(tx_uuid, tx_params)
    if tx_result.error != 0:
        _send_control(conn, type="error", message=f"TX characteristic error {tx_result.error}")
        return
    tx = tx_result.characteristic

    assembler = FrameAssembler()
    send_queue: List[bytes] = []
    max_payload = _DEFAULT_PAYLOAD
    subscribed = False

    async def _flush() -> None:
        while send_queue:
            packet = send_queue.pop(0)
            try:
                await tx.notify_value_async(_buffer_from_bytes(packet))
            except Exception:
                send_queue.insert(0, packet)
                return

    async def _on_write(args: Any, deferral: Any) -> None:
        try:
            request = await args.get_request_async()
            for frame in assembler.feed(_bytes_from_buffer(request.value)):
                conn.send_bytes(b"D" + frame)
            if request.option == GattWriteOption.WRITE_WITH_RESPONSE:
                request.respond()
        except Exception:
            logger.exception("BLE write request failed")
        finally:
            try:
                deferral.complete()
            except Exception:
                pass

    def _write_requested(sender: Any, args: Any) -> None:
        # Hold the request open until the async handler has responded;
        # otherwise Windows commits it as soon as this callback returns.
        deferral = args.get_deferral()
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(_on_write(args, deferral))
        )

    def _subscribed_changed(sender: Any, args: Any) -> None:
        nonlocal subscribed
        try:
            clients = list(tx.subscribed_clients)
        except Exception:
            clients = []
        if clients and not subscribed:
            subscribed = True
            _send_control(conn, type="opened", role="listen")
        elif not clients and subscribed:
            subscribed = False
            _send_control(conn, type="closed")

    rx.add_write_requested(_write_requested)
    tx.add_subscribed_clients_changed(_subscribed_changed)

    advertising = GattServiceProviderAdvertisingParameters()
    advertising.is_connectable = True
    advertising.is_discoverable = True
    try:
        advertising.use_low_energy_uncoded_2m_phy_as_secondary_phy = True
    except Exception:
        pass
    try:
        provider.start_advertising(advertising)
    except TypeError:
        # PyWinRT v3 exposed the parameterized overload under this name.
        provider.start_advertising_with_parameters(advertising)
    _send_control(conn, type="ready", role="listen")

    async def _on_frame(frame: bytes) -> None:
        send_queue.extend(fragment_frame(frame, max_payload))
        await _flush()

    try:
        await _pipe_reader(conn, _on_frame)
    finally:
        try:
            provider.stop_advertising()
        except Exception:
            pass


async def _run_central(conn: Connection, target: Optional[str]) -> None:
    from winrt.windows.devices.bluetooth import (
        BluetoothCacheMode,
        BluetoothLEDevice,
    )
    from winrt.windows.devices.bluetooth.advertisement import (
        BluetoothLEAdvertisementWatcher,
        BluetoothLEScanningMode,
    )
    from winrt.windows.devices.bluetooth.genericattributeprofile import (
        GattClientCharacteristicConfigurationDescriptorValue,
        GattSession,
    )

    service_uuid = uuid.UUID(BLE_SERVICE_UUID)
    rx_uuid = uuid.UUID(BLE_RX_UUID)
    tx_uuid = uuid.UUID(BLE_TX_UUID)

    loop = asyncio.get_running_loop()
    found: asyncio.Future = loop.create_future()
    watcher = BluetoothLEAdvertisementWatcher()
    watcher.scanning_mode = BluetoothLEScanningMode.ACTIVE

    def _received(sender: Any, args: Any) -> None:
        if found.done():
            return
        address = args.bluetooth_address
        name = args.advertisement.local_name or ""
        mac = _address_to_mac(address)
        uuids = {str(u).lower() for u in (args.advertisement.service_uuids or [])}
        if BLE_SERVICE_UUID not in uuids:
            return
        if target and target.lower() not in (
            str(address).lower(),
            mac.lower(),
            name.lower(),
        ):
            return
        loop.call_soon_threadsafe(found.set_result, (address, name))

    watcher.add_received(_received)
    watcher.start()
    try:
        address, _name = await asyncio.wait_for(found, 30.0)
    finally:
        watcher.stop()

    device = await BluetoothLEDevice.from_bluetooth_address_async(address)
    if device is None:
        _send_control(conn, type="error", message="BLE device not found")
        return

    try:
        session = await GattSession.from_device_id_async(device.bluetooth_device_id)
        session.maintain_connection = True
    except Exception:
        session = None

    services_result = await device.get_gatt_services_for_uuid_with_cache_mode_async(
        service_uuid, BluetoothCacheMode.UNCACHED
    )
    if services_result.status != 0 or not services_result.services:
        _send_control(conn, type="error", message="gti BLE service not found")
        return
    service = services_result.services[0]

    rx_result = await service.get_characteristics_for_uuid_with_cache_mode_async(
        rx_uuid, BluetoothCacheMode.UNCACHED
    )
    tx_result = await service.get_characteristics_for_uuid_with_cache_mode_async(
        tx_uuid, BluetoothCacheMode.UNCACHED
    )
    if not rx_result.characteristics or not tx_result.characteristics:
        _send_control(conn, type="error", message="gti BLE characteristics not found")
        return
    rx = rx_result.characteristics[0]
    tx = tx_result.characteristics[0]

    assembler = FrameAssembler()
    write_queue: List[bytes] = []
    writing = False
    max_payload = _DEFAULT_PAYLOAD

    async def _flush() -> None:
        nonlocal writing
        while write_queue and not writing:
            packet = write_queue.pop(0)
            writing = True
            try:
                result = await rx.write_value_with_result_async(
                    _buffer_from_bytes(packet)
                )
                if result.status != 0:
                    _send_control(
                        conn, type="error", message=f"BLE write status {result.status}"
                    )
                    return
            finally:
                writing = False

    async def _on_value_changed(args: Any) -> None:
        for frame in assembler.feed(_bytes_from_buffer(args.characteristic_value)):
            conn.send_bytes(b"D" + frame)

    def _value_changed(sender: Any, args: Any) -> None:
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(_on_value_changed(args))
        )

    tx.add_value_changed(_value_changed)
    await tx.write_client_characteristic_configuration_descriptor_async(
        GattClientCharacteristicConfigurationDescriptorValue.NOTIFY
    )
    _send_control(conn, type="opened", role="connect")

    async def _on_frame(frame: bytes) -> None:
        write_queue.extend(fragment_frame(frame, max_payload))
        await _flush()

    try:
        await _pipe_reader(conn, _on_frame)
    finally:
        try:
            device.close()
        except Exception:
            pass
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


async def _windows_main(conn: Connection, role: str, target: Optional[str]) -> None:
    try:
        if role == "listen":
            await _run_peripheral(conn)
        else:
            await _run_central(conn, target)
    except Exception as exc:
        logger.exception("Windows BLE transport failed")
        try:
            _send_control(conn, type="error", message=str(exc))
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ble_process_windows(conn: Connection, role: str, target: Optional[str]) -> None:
    asyncio.run(_windows_main(conn, role, target))


async def _scan_main(conn: Connection, timeout: float) -> None:
    from winrt.windows.devices.bluetooth.advertisement import (
        BluetoothLEAdvertisementWatcher,
        BluetoothLEScanningMode,
    )

    service_uuid = uuid.UUID(BLE_SERVICE_UUID)
    watcher = BluetoothLEAdvertisementWatcher()
    watcher.scanning_mode = BluetoothLEScanningMode.ACTIVE
    seen: set = set()

    def _received(sender: Any, args: Any) -> None:
        address = args.bluetooth_address
        if address in seen:
            return
        uuids = {str(u).lower() for u in (args.advertisement.service_uuids or [])}
        if BLE_SERVICE_UUID not in uuids:
            return
        seen.add(address)
        mac = _address_to_mac(address)
        name = args.advertisement.local_name or mac
        _send_control(
            conn,
            type="scan_result",
            name=f"{name} ({mac})",
            address=str(address),
            rssi=int(args.raw_signal_strength_in_dbm or 0),
        )

    watcher.add_received(_received)
    watcher.start()
    try:
        await asyncio.sleep(timeout)
    finally:
        watcher.stop()
    _send_control(conn, type="scan_done")


def ble_scan_process_windows(conn: Connection, timeout: float) -> None:
    try:
        asyncio.run(_scan_main(conn, timeout))
    except Exception as exc:
        try:
            _send_control(conn, type="error", message=str(exc))
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
