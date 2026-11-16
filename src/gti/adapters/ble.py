"""Bluetooth Low Energy (GATT) transport.

The BLE stack runs in a child process whose main thread owns the NSRunLoop.
The parent process talks to it over a multiprocessing pipe, so the existing
asyncio peer/protocol/services are unchanged.

GATT layout:

* service ``e6031152-a17a-4f76-9aa5-cba1b4321dcf``
* RX characteristic ``...0001`` — central writes to peripheral
* TX characteristic ``...0002`` — peripheral notifies central

Frames are length-prefixed and fragmented into BLE-sized packets by
:mod:`gti.adapters.ble_framing`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import sys
import threading
import time
from multiprocessing.connection import Connection
from typing import Any, List, Optional

from ..discovery import Device
from ..transport import Transport, TransportClosed
from .ble_framing import FrameAssembler, fragment_frame

__all__ = ["BLETransport", "discover_ble", "BLE_SERVICE_UUID"]

logger = logging.getLogger(__name__)

BLE_SERVICE_UUID = "e6031152-a17a-4f76-9aa5-cba1b4321dcf"
BLE_RX_UUID = "e6031152-a17a-4f76-9aa5-cba1b4321dc1"
BLE_TX_UUID = "e6031152-a17a-4f76-9aa5-cba1b4321dc2"

_DEFAULT_PAYLOAD = 180


def _require_ble_supported() -> None:
    if sys.platform not in ("darwin", "win32"):
        raise RuntimeError(
            "BLE transport is currently implemented on macOS and Windows; "
            "use --transport rfcomm on this platform"
        )


def _send_control(conn: Connection, **payload: Any) -> None:
    conn.send_bytes(b"C" + json.dumps(payload).encode())


def _as_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    return bytes(value)


def _ble_process(conn: Connection, role: str, target: Optional[str]) -> None:
    """Child process: run CoreBluetooth on the main thread and proxy frames."""
    import CoreBluetooth as CB  # type: ignore
    from Foundation import NSDate, NSObject, NSRunLoop  # type: ignore

    try:
        from AppKit import NSApplication  # type: ignore

        NSApplication.sharedApplication()
    except Exception:
        pass

    service_uuid = CB.CBUUID.UUIDWithString_(BLE_SERVICE_UUID)
    rx_uuid = CB.CBUUID.UUIDWithString_(BLE_RX_UUID)
    tx_uuid = CB.CBUUID.UUIDWithString_(BLE_TX_UUID)

    class _PeripheralDelegate(NSObject):
        def peripheralManagerDidUpdateState_(self, manager: Any) -> None:
            if manager.state() != CB.CBPeripheralManagerStatePoweredOn:
                if manager.state() == CB.CBPeripheralManagerStatePoweredOff:
                    _send_control(conn, type="error", message="Bluetooth is off")
                return
            service = CB.CBMutableService.alloc().initWithType_primary_(
                service_uuid, True
            )
            rx = CB.CBMutableCharacteristic.alloc().initWithType_properties_value_permissions_(
                rx_uuid,
                CB.CBCharacteristicPropertyWrite
                | CB.CBCharacteristicPropertyWriteWithoutResponse,
                None,
                CB.CBAttributePermissionsWriteable,
            )
            tx = CB.CBMutableCharacteristic.alloc().initWithType_properties_value_permissions_(
                tx_uuid,
                CB.CBCharacteristicPropertyNotify,
                None,
                CB.CBAttributePermissionsReadable,
            )
            service.setCharacteristics_([rx, tx])
            self.service = service
            self.rx = rx
            self.tx = tx
            manager.addService_(service)

        def peripheralManager_didAddService_error_(
            self, manager: Any, service: Any, error: Any
        ) -> None:
            if error is not None:
                _send_control(conn, type="error", message=str(error))
                return
            manager.startAdvertising_(
                {
                    CB.CBAdvertisementDataServiceUUIDsKey: [service_uuid],
                    CB.CBAdvertisementDataLocalNameKey: "gti",
                }
            )

        def peripheralManagerDidStartAdvertising_error_(
            self, manager: Any, error: Any
        ) -> None:
            if error is not None:
                _send_control(conn, type="error", message=str(error))
                return
            _send_control(conn, type="ready", role="listen")

        def peripheralManager_didReceiveWriteRequests_(
            self, manager: Any, requests: Any
        ) -> None:
            for request in requests:
                characteristic = request.characteristic()
                if characteristic.UUID().UUIDString().lower() == BLE_RX_UUID:
                    for frame in self.assembler.feed(_as_bytes(request.value())):
                        conn.send_bytes(b"D" + frame)
                manager.respondToRequest_withResult_(
                    request, CB.CBATTErrorSuccess
                )

        def peripheralManager_central_didSubscribeToCharacteristic_(
            self, manager: Any, central: Any, characteristic: Any
        ) -> None:
            if characteristic.UUID().UUIDString().lower() != BLE_TX_UUID:
                return
            self.centrals.append(central)
            try:
                length = int(central.maximumUpdateValueLength())
            except Exception:
                length = _DEFAULT_PAYLOAD
            self.max_payload = max(20, min(512, length))
            _send_control(conn, type="opened", role="listen")

        def peripheralManager_central_didUnsubscribeFromCharacteristic_(
            self, manager: Any, central: Any, characteristic: Any
        ) -> None:
            if central in self.centrals:
                self.centrals.remove(central)
            if not self.centrals:
                _send_control(conn, type="closed")

        def peripheralManagerIsReadyToUpdateSubscribers_(self, manager: Any) -> None:
            self._flush()

        def _flush(self) -> None:
            while self.send_queue and self.centrals:
                packet = self.send_queue[0]
                ok = self.manager.updateValue_forCharacteristic_onSubscribedCentrals_(
                    packet, self.tx, None
                )
                if not ok:
                    return
                self.send_queue.pop(0)

        def send_frame(self, frame: bytes) -> None:
            self.send_queue.extend(fragment_frame(frame, self.max_payload))
            self._flush()

    class _CentralDelegate(NSObject):
        peripheral: Any

        def centralManagerDidUpdateState_(self, manager: Any) -> None:
            if manager.state() != CB.CBCentralManagerStatePoweredOn:
                if manager.state() == CB.CBCentralManagerStatePoweredOff:
                    _send_control(conn, type="error", message="Bluetooth is off")
                return
            manager.scanForPeripheralsWithServices_options_(
                [service_uuid], None
            )

        def centralManager_didDiscoverPeripheral_advertisementData_RSSI_(
            self, manager: Any, peripheral: Any, data: Any, rssi: Any
        ) -> None:
            if self.peripheral is not None:
                return
            name = peripheral.name() or data.get(
                CB.CBAdvertisementDataLocalNameKey, ""
            )
            identifier = peripheral.identifier().UUIDString()
            if target and target.lower() not in (
                identifier.lower(),
                str(name).lower(),
            ):
                return
            self.peripheral = peripheral
            manager.stopScan()
            peripheral.setDelegate_(self)
            manager.connectPeripheral_options_(peripheral, None)

        def centralManager_didConnectPeripheral_(self, manager: Any, peripheral: Any) -> None:
            peripheral.discoverServices_([service_uuid])

        def centralManager_didFailToConnectPeripheral_error_(
            self, manager: Any, peripheral: Any, error: Any
        ) -> None:
            _send_control(conn, type="error", message=str(error))

        def centralManager_didDisconnectPeripheral_error_(
            self, manager: Any, peripheral: Any, error: Any
        ) -> None:
            _send_control(conn, type="closed")

        def peripheral_didDiscoverServices_(
            self, peripheral: Any, error: Any
        ) -> None:
            if error is not None:
                _send_control(conn, type="error", message=str(error))
                return
            for service in peripheral.services() or []:
                if service.UUID().UUIDString().lower() == BLE_SERVICE_UUID:
                    peripheral.discoverCharacteristics_forService_(
                        [rx_uuid, tx_uuid], service
                    )
                    return
            _send_control(conn, type="error", message="gti BLE service not found")

        def peripheral_didDiscoverCharacteristicsForService_error_(
            self, peripheral: Any, service: Any, error: Any
        ) -> None:
            if error is not None:
                _send_control(conn, type="error", message=str(error))
                return
            for characteristic in service.characteristics() or []:
                uuid = characteristic.UUID().UUIDString().lower()
                if uuid == BLE_RX_UUID:
                    self.rx = characteristic
                elif uuid == BLE_TX_UUID:
                    self.tx = characteristic
            if self.rx is None or self.tx is None:
                _send_control(conn, type="error", message="gti BLE characteristics not found")
                return
            try:
                length = int(
                    peripheral.maximumWriteValueLengthForType_(
                        CB.CBCharacteristicWriteWithResponse
                    )
                )
            except Exception:
                length = _DEFAULT_PAYLOAD
            self.max_payload = max(20, min(512, length))
            peripheral.setNotifyValue_forCharacteristic_(True, self.tx)

        def peripheral_didUpdateNotificationStateForCharacteristic_error_(
            self, peripheral: Any, characteristic: Any, error: Any
        ) -> None:
            if error is not None:
                _send_control(conn, type="error", message=str(error))
                return
            if characteristic.isNotifying():
                _send_control(conn, type="opened", role="connect")

        def peripheral_didUpdateValueForCharacteristic_error_(
            self, peripheral: Any, characteristic: Any, error: Any
        ) -> None:
            if error is not None:
                return
            if characteristic.UUID().UUIDString().lower() != BLE_TX_UUID:
                return
            for frame in self.assembler.feed(_as_bytes(characteristic.value())):
                conn.send_bytes(b"D" + frame)

        def peripheral_didWriteValueForCharacteristic_error_(
            self, peripheral: Any, characteristic: Any, error: Any
        ) -> None:
            self.writing = False
            if error is not None:
                _send_control(conn, type="error", message=str(error))
                return
            self._flush()

        def _flush(self) -> None:
            while self.write_queue and not self.writing:
                packet = self.write_queue.pop(0)
                self.writing = True
                self.peripheral.writeValue_forCharacteristic_type_(
                    packet, self.rx, CB.CBCharacteristicWriteWithResponse
                )

        def send_frame(self, frame: bytes) -> None:
            self.write_queue.extend(fragment_frame(frame, self.max_payload))
            self._flush()

    try:
        if role == "listen":
            delegate = _PeripheralDelegate.alloc().init()
            delegate.assembler = FrameAssembler()
            delegate.centrals = []
            delegate.send_queue = []
            delegate.max_payload = _DEFAULT_PAYLOAD
            delegate.service = None
            delegate.rx = None
            delegate.tx = None
            manager = CB.CBPeripheralManager.alloc().initWithDelegate_queue_(
                delegate, None
            )
            delegate.manager = manager
        else:
            delegate = _CentralDelegate.alloc().init()
            delegate.assembler = FrameAssembler()
            delegate.write_queue = []
            delegate.writing = False
            delegate.max_payload = _DEFAULT_PAYLOAD
            delegate.peripheral = None
            delegate.rx = None
            delegate.tx = None
            manager = CB.CBCentralManager.alloc().initWithDelegate_queue_(
                delegate, None
            )
            delegate.manager = manager

        def _reader() -> None:
            while True:
                try:
                    frame = conn.recv_bytes()
                except (EOFError, OSError):
                    break
                try:
                    delegate.send_frame(frame)
                except Exception:
                    break
            delegate.closed = True

        delegate.closed = False
        threading.Thread(target=_reader, daemon=True, name="gti-ble-writer").start()

        run_loop = NSRunLoop.currentRunLoop()
        while not delegate.closed:
            run_loop.runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))
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


def _ble_scan_process(conn: Connection, timeout: float) -> None:
    """Child process: scan for gti BLE peripherals and report them."""
    import CoreBluetooth as CB
    from Foundation import NSDate, NSObject, NSRunLoop

    service_uuid = CB.CBUUID.UUIDWithString_(BLE_SERVICE_UUID)
    seen: set = set()

    class _ScanDelegate(NSObject):
        def centralManagerDidUpdateState_(self, manager: Any) -> None:
            if manager.state() == CB.CBCentralManagerStatePoweredOn:
                manager.scanForPeripheralsWithServices_options_(
                    [service_uuid], None
                )
            elif manager.state() == CB.CBCentralManagerStatePoweredOff:
                _send_control(conn, type="error", message="Bluetooth is off")

        def centralManager_didDiscoverPeripheral_advertisementData_RSSI_(
            self, manager: Any, peripheral: Any, data: Any, rssi: Any
        ) -> None:
            identifier = peripheral.identifier().UUIDString()
            if identifier in seen:
                return
            seen.add(identifier)
            name = peripheral.name() or data.get(
                CB.CBAdvertisementDataLocalNameKey, ""
            )
            _send_control(
                conn,
                type="scan_result",
                name=str(name or identifier),
                address=identifier,
                rssi=int(rssi) if rssi is not None else 0,
            )

    try:
        delegate = _ScanDelegate.alloc().init()
        manager = CB.CBCentralManager.alloc().initWithDelegate_queue_(
            delegate, None
        )
        run_loop = NSRunLoop.currentRunLoop()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            run_loop.runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))
        manager.stopScan()
        _send_control(conn, type="scan_done")
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


class BLETransport(Transport):
    """BLE GATT transport backed by a CoreBluetooth child process."""

    def __init__(
        self,
        *,
        role: str = "listen",
        target: Optional[str] = None,
        timeout: float = 180.0,
    ) -> None:
        super().__init__()
        _require_ble_supported()
        if role not in ("listen", "connect"):
            raise ValueError("role must be 'listen' or 'connect'")
        self._role = role
        self._target = target
        self._timeout = timeout
        self._conn: Optional[Connection] = None
        self._proc: Optional[Any] = None
        self._recv_task: Optional["asyncio.Task[None]"] = None
        self._opened_future: Optional["asyncio.Future[None]"] = None
        self._closed = False

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        if self._closed:
            raise TransportClosed("BLE transport is closed")
        loop = asyncio.get_running_loop()
        ctx = multiprocessing.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe(duplex=True)
        if sys.platform == "win32":
            from .ble_windows import ble_process_windows

            target = ble_process_windows
        else:
            target = _ble_process
        proc = ctx.Process(
            target=target,
            args=(child_conn, self._role, self._target),
            daemon=True,
        )
        proc.start()
        child_conn.close()
        self._conn = parent_conn
        self._proc = proc
        self._opened_future = loop.create_future()
        self._recv_task = asyncio.create_task(self._recv_loop())
        try:
            await asyncio.wait_for(self._opened_future, self._timeout)
        except asyncio.TimeoutError:
            await self.close()
            raise ConnectionError(
                "timed out waiting for a BLE connection"
            ) from None
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception:
            await self.close()
            raise

    async def _recv_loop(self) -> None:
        loop = asyncio.get_running_loop()
        conn = self._conn
        if conn is None:
            return
        while not self._closed:
            try:
                data = await loop.run_in_executor(None, conn.recv_bytes)
            except (EOFError, OSError):
                break
            if not data:
                continue
            if data.startswith(b"D"):
                await self._dispatch(data[1:])
            elif data.startswith(b"C"):
                try:
                    message = json.loads(data[1:].decode())
                except ValueError:
                    continue
                kind = message.get("type")
                if kind == "opened":
                    future = self._opened_future
                    if future is not None and not future.done():
                        future.set_result(None)
                elif kind == "closed":
                    future = self._opened_future
                    if future is not None and not future.done():
                        future.set_exception(ConnectionError("BLE link closed"))
                    break
                elif kind == "error":
                    future = self._opened_future
                    if future is not None and not future.done():
                        future.set_exception(
                            ConnectionError(
                                str(message.get("message", "BLE transport failed"))
                            )
                        )
                    logger.error("BLE transport: %s", message.get("message"))
                    break
        future = self._opened_future
        if future is not None and not future.done():
            future.set_exception(
                ConnectionError("BLE transport closed before connecting")
            )
        if not self._closed:
            self._closed = True
            await self._notify_close()

    async def send(self, frame: bytes) -> None:
        if self._closed or self._conn is None:
            raise TransportClosed("BLE transport is not connected")
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._conn.send_bytes, frame)
        except (EOFError, OSError) as exc:
            raise TransportClosed(f"BLE transport is not connected: {exc}") from exc

    async def close(self) -> None:
        if self._closed and self._conn is None and self._proc is None:
            return
        self._closed = True
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        future = self._opened_future
        self._opened_future = None
        if future is not None and not future.done():
            future.cancel()
        task = self._recv_task
        self._recv_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        proc = self._proc
        self._proc = None
        if proc is not None:
            proc.join(timeout=2)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2)
            if proc.is_alive():
                proc.kill()
        await self._notify_close()


async def discover_ble(timeout: float = 15.0) -> List[Device]:
    """Scan for gti BLE peripherals."""
    _require_ble_supported()
    loop = asyncio.get_running_loop()
    ctx = multiprocessing.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    if sys.platform == "win32":
        from .ble_windows import ble_scan_process_windows

        target = ble_scan_process_windows
    else:
        target = _ble_scan_process
    proc = ctx.Process(
        target=target,
        args=(child_conn, timeout),
        daemon=True,
    )
    proc.start()
    child_conn.close()
    devices: List[Device] = []
    deadline = time.monotonic() + timeout + 5

    def _recv() -> Optional[bytes]:
        if not parent_conn.poll(max(0.1, deadline - time.monotonic())):
            return None
        return parent_conn.recv_bytes()

    try:
        while time.monotonic() < deadline:
            data = await loop.run_in_executor(None, _recv)
            if data is None:
                continue
            if not data.startswith(b"C"):
                continue
            message = json.loads(data[1:].decode())
            if message.get("type") == "scan_result":
                devices.append(
                    Device(
                        name=str(message.get("name", "")),
                        address=str(message.get("address", "")),
                    )
                )
            elif message.get("type") == "scan_done":
                break
            elif message.get("type") == "error":
                raise RuntimeError(message.get("message", "BLE scan failed"))
    finally:
        try:
            parent_conn.close()
        except Exception:
            pass
        proc.join(timeout=2)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=2)
        if proc.is_alive():
            proc.kill()
    return devices
