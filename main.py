import socket as socket_lib
import os as os_lib
from datetime import datetime as datetime_lib
import signal as signal_lib
import asyncio as asyncio_lib
from concurrent.futures import ThreadPoolExecutor as ThreadPoolExecutor_lib

# Папка для сохранения логов
LOG_DIR = "incoming"
os_lib.makedirs(LOG_DIR, exist_ok=True)

# Настройка сервера
HOST = "0.0.0.0"
PORT = 514
MAX_CONN = 10
BUFFER_SIZE = 2048

# настройки многопоточности и асинхронности
MAX_THREADS = 8
THREADS_EXECUTOR = ThreadPoolExecutor_lib(max_workers=MAX_THREADS)

# Обработка одного любого отдельно взятого события
def process_event(event_data):
    """Обработка события.
    Сохраняет данные, если они содержат 'scan_machine.final_result'.
    """
    if event_data:
        timestamp = datetime_lib.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"event_{timestamp}.log"
        filepath = os_lib.path.join(LOG_DIR, filename)
        with open(filepath, "w") as log_file:
            log_file.write(event_data)
        if "scan_machine.final_result" in event_data:
            print(f"Saved event to {filename}  || Contains <scan_machine.final_result>")
        else:
            print(f"Saved event to {filename}")
    else:
        print("No Event data")

# обработка каждого клиентского подключения
async def handle_client_connection(client_socket, loop):
    """Обрабатывает входящие данные от клиента построчно.
    Асинхронно разделяет задачи на потоки
    """
    buffer = ""
    try:
        while True:
            recieved_data = await loop.sock_recv(client_socket, BUFFER_SIZE)
            if not recieved_data:
                break
            buffer += recieved_data.decode("utf-8")

            while "\n" in buffer:
                current_line, buffer = buffer.split("\n", 1)
                await loop.run_in_executor(THREADS_EXECUTOR, process_event, current_line.strip())
    except Exception as e:
        print(f"Error! While handling client connection:\n{e}")
    finally:
        client_socket.close()

    # если остались данные в буфере после разрыва соединения
    if buffer.strip():
        await loop.run_in_executor(THREADS_EXECUTOR, process_event, buffer.strip())

# запуск сервера-листенера
async def start_server():
    """Запускает TCP-сервер.
    """
    loop = asyncio_lib.get_running_loop()
    server_socket = socket_lib.socket(socket_lib.AF_INET, socket_lib.SOCK_STREAM)
    server_socket.setsockopt(socket_lib.SOL_SOCKET, socket_lib.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    server_socket.setblocking(False)

    print(f"Server is listening on {HOST}:{PORT}...")

    try:
        while True:
            client_socket, remote_host = await loop.sock_accept(server_socket)
            print(f"Connection established with {remote_host}")
            loop.create_task(handle_client_connection(client_socket, loop))
    except asyncio_lib.CancelledError:
        print("Server shutdown in progress...")
    finally:
        server_socket.close()
        print("Server socket closed.")

# асинхронный обработчик сигнала
async def shutdown_server():
    print("\nReceived shutdown signal. Closing server gracefully...\n")
    TASKS = [task for task in asyncio_lib.all_tasks() if task is not asyncio_lib.current_task()]
    for task in TASKS:
        task.cancel()
    await asyncio_lib.gather(*TASKS, return_exceptions=True)
    THREADS_EXECUTOR.shutdown(wait=True)
    print("\nGraceful shutdown complete.\n")

# добавление сигналов в цикл
def setup_signals():
    loop = asyncio_lib.get_running_loop()
    for sig in (signal_lib.SIGINT, signal_lib.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio_lib.create_task(shutdown_server()))

if __name__ == "__main__":
    try:
        asyncio_lib.run(start_server())
    except KeyboardInterrupt:
        print("\nServer stopped by user.")
