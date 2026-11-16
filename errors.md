gti chat --scan
Select transport:
  1. Bluetooth Classic (RFCOMM)
  2. Bluetooth Low Energy (BLE)
Transport [1]: 2
Scanning for BLE devices (15s) …
Bluetooth devices:
  1. gti (60:3E:5F:33:C4:DF)  [105821001467103] (found)
  2. gti (5C:52:30:9B:24:9B)  [101508072547483] (found)
Select device number (or type an address, blank to cancel): 1
Selected gti (60:3E:5F:33:C4:DF) (105821001467103)
Connecting to BLE device 105821001467103 …
Windows BLE transport failed
Traceback (most recent call last):
  File "\gti\src\gti\adapters\ble_windows.py", line 338, in _windows_main
    await _run_central(conn, target)
  File "\gti\src\gti\adapters\ble_windows.py", line 258, in _run_central
    services_result = await device.get_gatt_services_for_uuid_async(
                            ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^
        service_uuid, BluetoothCacheMode.UNCACHED
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
TypeError: Invalid parameter count
gti: BLE transport: Invalid parameter count
gti: Invalid parameter count