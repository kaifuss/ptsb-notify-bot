import socket
import os
from datetime import datetime

# Папка для сохранения логов
LOG_DIR = "incoming"
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка сервера
HOST = "0.0.0.0"
PORT = 514

def process_event(event_data):
    """Обработка события.
    Сохраняет данные, если они содержат 'scan_machine.final_result'.
    """
    if "scan_machine.final_result" in event_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"event_{timestamp}.log"
        filepath = os.path.join(LOG_DIR, filename)
        with open(filepath, "w") as log_file:
            log_file.write(event_data)
        print(f"Saved event to {filename}")
    else:
        print("Event does not match 'scan_machine.final_result'. Ignored.")

def handle_client_connection(client_socket):
    """
    Обрабатывает входящие данные от клиента построчно.
    """
    buffer = ""
    while True:
        data = client_socket.recv(1024)
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
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((HOST, PORT))
            server_socket.listen(5)
            print(f"Server is listening on {HOST}:{PORT}...")
            while True:
                client_socket, addr = server_socket.accept()
                print(f"Connection established with {addr}")
                with client_socket:
                    handle_client_connection(client_socket)
    except KeyboardInterrupt:
        client_socket.close()
        server_socket.close()

if __name__ == "__main__":
    start_server()
