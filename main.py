import socket
import json
import re
import os
from datetime import datetime

# Настройки
HOST = os.getenv('HOST_LOCAL_ADDRESS')        
PORT = os.getenv('HOST_LOCAL_PORT')
HOST = '0.0.0.0'
PORT = '514'
SAVE_DIR = "incoming"  # Папка для сохранения сырых событий

# создание папки
os.makedirs(SAVE_DIR, exist_ok=True)

def save_raw_event(data):
    """
    Сохраняет сырое событие в файл.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = os.path.join(SAVE_DIR, f"event_{timestamp}.log")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(data)

def parse_event(data):
    """
    Проверяет и парсит событие, если оно валидное JSON.
    """
    # Сохраняем сырое событие
    save_raw_event(data)

    try:
        # Удаляем префикс до JSON, если он существует
        match = re.search(r'\{.*\}', data)
        if not match:
            print("Invalid JSON received.")
            return None

        json_data = json.loads(match.group(0))
        return json_data
    except json.JSONDecodeError:
        print("Invalid JSON received.")
        return None

def handle_event(event):
    """
    Обрабатывает событие, если заголовок соответствует.
    """
    header = "scan_machine.final_result"
    if header not in event:
        print("Event does not match the required header. Ignored.")
        return

    print("Received event:")
    print(json.dumps(event, indent=4, ensure_ascii=False))

    # Обработка данных события
    scan_id = event.get("scan_id")
    result = event.get("result", {})
    verdict = result.get("verdict", {}).get("threat_level", "UNKNOWN")

    if verdict == "CLEAN":
        print(f"Scan {scan_id}: No threat detected.")
    else:
        print(f"Scan {scan_id}: Threat detected - {verdict}.")

def start_server():
    """
    Запускает TCP сервер для приема событий.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        print(f"Server is listening on {HOST}:{PORT}...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connection established with {client_address}")

            with client_socket:
                data = client_socket.recv(4096).decode("utf-8")
                if data:
                    event = parse_event(data)
                    if event:
                        handle_event(event)

if __name__ == "__main__":
    start_server()
