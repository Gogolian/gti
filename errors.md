> gti chat                  
Select transport:
  1. Bluetooth Classic (RFCOMM)
  2. Bluetooth Low Energy (BLE)
Transport [1]: 2
Waiting for BLE connection …
Windows BLE transport failed
Traceback (most recent call last):
  File "\gti\src\gti\adapters\ble_windows.py", line 314, in _windows_main
    await _run_peripheral(conn)
  File "\gti\src\gti\adapters\ble_windows.py", line 162, in _run_peripheral
    provider.start_advertising(advertising)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
TypeError: Invalid parameter count
gti: BLE transport: Invalid parameter count
gti: Invalid parameter count
> gti chat --scan
Select transport:
  1. Bluetooth Classic (RFCOMM)
  2. Bluetooth Low Energy (BLE)
Transport [1]: 2
Scanning for BLE devices (15s) …
Traceback (most recent call last):
  File "\gti\src\gti\adapters\ble_windows.py", line 360, in _received
    rssi=int(args.rssi or 0),
AttributeError: 'winrt._winrt_windows_devices_bluetooth_advertisement.BluetoothLEAdvertisementReceivedEventArgs' object has no attribute 'rssi'
No Bluetooth devices found.
gti: no device selected