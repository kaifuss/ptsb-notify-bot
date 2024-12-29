import socket
import os
from datetime import datetime
import signal
import asyncio
from concurrent.futures import ThreadPoolExecutor


# Папка для сохранения логов
LOG_DIR = "incoming"
os.makedirs(LOG_DIR, exist_ok=True)


# Настройка сервера
HOST = "0.0.0.0"
PORT = 514
MAX_CONN = 10
BUFFER_SIZE = 4096


# настройки многопоточности 
MAX_THREADS = 8
theads_executor = ThreadPoolExecutor(max_workers=MAX_THREADS)


# обработка одного любого отдельно взятого события
def process_event(event_data):
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

async def handle_client_connection(client_socket, loop):
    """
    Обрабатывает входящие данные от клиента построчно.
    Асинхронно разделяет задачи на потоки
    """
    
    buffer = ""
    while True:
        try:
            recieved_data = await loop.sock_recv(client_socket, BUFFER_SIZE)
            if not recieved_data:
                break
            buffer += recieved_data.decode("utf-8")

            while "\n" in buffer:
                current_line, buffer = buffer.split("\n", 1)
                await loop.run_in_executor(theads_executor, process_event, current_line.strip())
        except Exception as e:
            print(f"Error! While handling client connection:\n{e}")
            break
    
    # если остались данные в буфере после разрыва соединения
    if buffer.strip():
        await loop.run_in_executor(theads_executor, process_event, buffer.strip())

# запуск сервера-листенера
async def start_server():
    """
    Запускает TCP-сервер.
    """
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    server_socket.setblocking(False)

    loop = asyncio.get_event_loop()

    print(f"Server is listening on {HOST}:{PORT}...")

    while True:
        client_socket, remote_host = server_socket.accept()
        print(f"Connection established with {remote_host}")
        loop.create_task(handle_client_connection(client_socket, loop))


# # обработчик закрытия сервера
# def shutdown_server(signal_num, frame):
#     print(f"Recevied signal: {signal_num}")
#     global server_socket
#     if server_socket:
#         server_socket.close()
#         print("\nServer was shutted down gracefully.\n")
#     sys.exit(0)

if __name__ == "__main__":

    # запуск сервера в асинхронном режиме
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        print("\n\nServer shutted down\n")
    # # обработчики для завершения сервера gracefully
    # signal.signal(signal.SIGINT, shutdown_server)
    # signal.signal(signal.SIGTERM, shutdown_server)

    # # запуск листенера
    # start_server()