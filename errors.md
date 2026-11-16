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
Process SpawnProcess-1:
Traceback (most recent call last):
  File "\Python3.14.6\Lib\asyncio\runners.py", line 128, in run
    return self._loop.run_until_complete(task)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^
  File "\Python3.14.6\Lib\asyncio\base_events.py", line 719, in run_until_complete
    return future.result()
           ~~~~~~~~~~~~~^^
  File "\gti\src\gti\adapters\ble_windows.py", line 322, in _windows_main
    await _run_peripheral(conn)
  File "\gti\src\gti\adapters\ble_windows.py", line 178, in _run_peripheral
    await _pipe_reader(conn, _on_frame)
  File "\gti\src\gti\adapters\ble_windows.py", line 66, in _pipe_reader
    frame = await loop.run_in_executor(None, conn.recv_bytes)
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "\Python3.14.6\Lib\multiprocessing\process.py", line 320, in _bootstrap
    self.run()
    ~~~~~~~~^^
  File "\Python3.14.6\Lib\multiprocessing\process.py", line 108, in run
    self._target(*self._args, **self._kwargs)
    ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "\gti\src\gti\adapters\ble_windows.py", line 339, in ble_process_windows
    asyncio.run(_windows_main(conn, role, target))
    ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "\Python3.14.6\Lib\asyncio\runners.py", line 205, in run
    return runner.run(main)
           ~~~~~~~~~~^^^^^^
  File "\Python3.14.6\Lib\asyncio\runners.py", line 133, in run
    raise KeyboardInterrupt()
KeyboardInterrupt
Traceback (most recent call last):
  File "\Python3.14.6\Lib\asyncio\runners.py", line 128, in run
    return self._loop.run_until_complete(task)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^
  File "\Python3.14.6\Lib\asyncio\base_events.py", line 719, in run_until_complete
    return future.result()
           ~~~~~~~~~~~~~^^
  File "\gti\src\gti\cli.py", line 261, in _run_chat
    await _run_session(transport_obj, role=role, receive_dir=receive_dir)
  File "\gti\src\gti\cli.py", line 143, in _run_session
    line = (await loop.run_in_executor(None, input_fn, "> ")).strip()
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "<frozen runpy>", line 203, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "\Python3.14.6\Scripts\gti.exe\__main__.py", line 5, in <module>
    sys.exit(main())
             ~~~~^^
  File "\gti\src\gti\cli.py", line 394, in main
    asyncio.run(
    ~~~~~~~~~~~^
        _run_chat(
        ^^^^^^^^^^
    ...<7 lines>...
        )
        ^
    )
    ^
  File "\Python3.14.6\Lib\asyncio\runners.py", line 205, in run
    return runner.run(main)
           ~~~~~~~~~~^^^^^^
  File "\Python3.14.6\Lib\asyncio\runners.py", line 133, in run
    raise KeyboardInterrupt()
KeyboardInterrupt
PS \gti> gti chat --transport ble
Waiting for BLE connection …
Connected.  Type a message, '/file PATH' to send a file, '/quit' to leave.
[peer] asd
> BLE write request failed
Traceback (most recent call last):
  File "\gti\src\gti\adapters\ble_windows.py", line 140, in _on_write
    request.respond()
    ~~~~~~~~~~~~~~~^^
OSError: [WinError -2147483618] The object has been committed.
