import asyncio as asyncio_lib
import socket as socket_lib
import os as os_lib
from datetime import datetime as datetime_lib
from concurrent.futures import ThreadPoolExecutor as ThreadPoolExecutor_lib
import signal as signal_lib

# Настройки
LOG_DIR = "incoming"
os_lib.makedirs(LOG_DIR, exist_ok=True)

HOST = "0.0.0.0"
PORT = 514
BUFFER_SIZE = 2048
SERVER_SOCKET = None
IS_SEVER_SHUTDOWN_INITIATED = False

MAX_THREADS = 8
THREADS_EXECUTOR = ThreadPoolExecutor_lib(max_workers=MAX_THREADS)


# Обработка одного события
def process_event(event_data):
    if event_data:
        timestamp = datetime_lib.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"event_{timestamp}.log"
        filepath = os_lib.path.join(LOG_DIR, filename)
        with open(filepath, "w") as log_file:
            log_file.write(event_data)
        if "scan_machine.final_result" in event_data:
            print(f"Saved event to {filename} || Contains <scan_machine.final_result>")
        else:
            print(f"Saved event to {filename}")


# Обработка подключения клиента
async def handle_client_connection(client_socket, loop):
    buffer = ""
    try:
        while True:
            received_data = await loop.sock_recv(client_socket, BUFFER_SIZE)
            if not received_data:
                break
            buffer += received_data.decode("utf-8")
            while "\n" in buffer:
                current_line, buffer = buffer.split("\n", 1)
                await loop.run_in_executor(THREADS_EXECUTOR, process_event, current_line.strip())
    except asyncio_lib.CancelledError:
        print("Task cancelled. Closing client connection.")
    except Exception as e:
        print(f"Error while handling client connection: {e}")
    finally:
        client_socket.close()
        if buffer.strip():
            await loop.run_in_executor(THREADS_EXECUTOR, process_event, buffer.strip())


# Запуск сервера
async def start_server():
    global SERVER_SOCKET
    SERVER_SOCKET = socket_lib.socket(socket_lib.AF_INET, socket_lib.SOCK_STREAM)
    SERVER_SOCKET.setsockopt(socket_lib.SOL_SOCKET, socket_lib.SO_REUSEADDR, 1)
    SERVER_SOCKET.bind((HOST, PORT))
    SERVER_SOCKET.listen()
    SERVER_SOCKET.setblocking(False)
    loop = asyncio_lib.get_running_loop()
    print(f"Server listening on {HOST}:{PORT}...")
    try:
        while True:
            client_socket, addr = await loop.sock_accept(SERVER_SOCKET)
            print(f"Connection established with {addr}")
            loop.create_task(handle_client_connection(client_socket, loop))
    except asyncio_lib.CancelledError:
        print("Server task cancelled.")
    finally:
        SERVER_SOCKET.close()
        print("Server socket closed.")


# Завершение сервера
async def shutdown_server():
    global IS_SEVER_SHUTDOWN_INITIATED
    if IS_SEVER_SHUTDOWN_INITIATED:
        return
    IS_SEVER_SHUTDOWN_INITIATED = True

    print("\nReceived shutdown signal. Closing server gracefully...")
    tasks = [t for t in asyncio_lib.all_tasks() if t is not asyncio_lib.current_task()]
    print(f"Cancelling {len(tasks)} tasks...")
    for task in tasks:
        task.cancel()
    await asyncio_lib.gather(*tasks, return_exceptions=True)
    THREADS_EXECUTOR.shutdown(wait=True)
    print("Server shutdown completed.")


# Настройка обработки сигналов
def setup_signals(loop):
    for sig in (signal_lib.SIGINT, signal_lib.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio_lib.create_task(shutdown_server()))


if __name__ == "__main__":
    loop = asyncio_lib.new_event_loop()
    asyncio_lib.set_event_loop(loop)
    setup_signals(loop)
    try:
        loop.run_until_complete(start_server())
    except (KeyboardInterrupt, SystemExit):
        print("Server stopped by user.")
    finally:
        loop.run_until_complete(shutdown_server())
        loop.close()
