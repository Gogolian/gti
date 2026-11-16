gti chat       
Select transport:
  1. Bluetooth Classic (RFCOMM)
  2. Bluetooth Low Energy (BLE)
Transport [1]: 2
Waiting for BLE connection …
Connected.  Type a message, '/file PATH' to send a file, '/quit' to leave.
> BLE write request failed
Traceback (most recent call last):
  File "\gti\src\gti\adapters\ble_windows.py", line 134, in _on_write
    for frame in assembler.feed(_bytes_from_buffer(request.value)):
                                ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File "\gti\src\gti\adapters\ble_windows.py", line 59, in _bytes_from_buffer
    return bytes(reader.read_bytes(buffer.length))
                 ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
TypeError: a bytes-like object is required, not 'int'