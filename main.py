import socket
import os
from datetime import datetime
import signal
import sys


# Папка для сохранения логов
LOG_DIR = "incoming"
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка сервера
HOST = "0.0.0.0"
PORT = 514
MAX_CONN = 20
BUFFER_SIZE = 4096


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

def handle_client_connection(client_socket):
    """
    Обрабатывает входящие данные от клиента построчно.
    """
    buffer = ""
    while True:
        data = client_socket.recv(BUFFER_SIZE)
        if not data:
            break
        buffer += data.decode("utf-8")
        
        # Разбиваем буфер на строки
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            process_event(line.strip())

    # Если остались данные в буфере после разрыва соединения
    if buffer.strip():
        process_event(buffer.strip())

def start_server():
    """
    Запускает TCP-сервер.
    """
    global server_socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen(MAX_CONN)
        print(f"Server is listening on {HOST}:{PORT}...")
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection established with {addr}")
            with client_socket:
                handle_client_connection(client_socket)


# обработчик закрытия сервера
def shutdown_server(signal_num, frame):
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
    start_server()
