import socket
import os
from datetime import datetime
import signal
import sys
import asyncio


# Папка для сохранения логов
LOG_DIR = "incoming"
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка сервера
HOST = "0.0.0.0"
PORT = 514
MAX_CONN = 20
BUFFER_SIZE = 4096


async def process_event(event_data):
    """Обработка события.
    Сохраняет данные, если они содержат 'scan_machine.final_result'.
    """
    if event_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"event_{timestamp}.log"
        filepath = os.path.join(LOG_DIR, filename)
        with open(filepath, "w") as log_file:
            log_file.write(event_data)
        if "scan_machine.final_result" in event_data:
            print(f"Saved event to {filename}  || Contains <scan_machine.final_result>")
        else:
            print(f"Saved event to {filename}")
    else:
        print("No Event data")


async def handle_client_connection(client_socket):
    """
    Обрабатывает входящие данные от клиента построчно.
    """
    buffer = ""
    while True:
        data = await asyncio.to_thread(client_socket.recv(BUFFER_SIZE))
        if not data:
            break
        buffer += data.decode("utf-8")
        
        # Разбиваем буфер на строки
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            await process_event(line.strip())

    # Если остались данные в буфере после разрыва соединения
    if buffer.strip():
        await process_event(buffer.strip())


# сервер листенер всего входящего прикола
async def start_server():
    """
    Запускает TCP-сервер.
    """
    global server_socket

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(HOST, PORT)
    server_socket.listen(MAX_CONN)
    server_socket.setblocking(False)

    print(f"Server is listening on {HOST}:{PORT}...")

    loop = asyncio.get_event_loop()

    while True:
        client_socket, addr = await loop.sock_accept(server_socket)
        print(f"Recieved connection from {addr}")
        asyncio.create_task(handle_client_connection(client_socket))


# обработчик закрытия сервера
def shutdown_server(signal_num, frame):
    """
    Хэндлер корректного завершения сервера
    """
    print(f"Recevied signal: {signal_num}")
    global server_socket
    if server_socket:
        server_socket.close()
        print("\nServer was shutted down gracefully.\n")
    sys.exit(0)

if __name__ == "__main__":
    # обработчики для завершения сервера gracefully
    signal.signal(signal.SIGINT, shutdown_server)
    signal.signal(signal.SIGTERM, shutdown_server)

    # запуск листенера
    asyncio.run(start_server())
