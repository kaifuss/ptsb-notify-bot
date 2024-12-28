import socket
import json
import re


# Настройки
HOST = '0.0.0.0'  # Слушать на всех интерфейсах
PORT = 514        # Порт для Syslog


# функция извлечения json части из строки
def extract_json(data):
    """
    Извлекает JSON-часть из строки.
    """
    try:
        # Ищем содержимое между первой открывающей и последней закрывающей фигурными скобками
        match = re.search(r'\{.*\}', data, re.DOTALL)
        if match:
            json_data = match.group(0)
            return json.loads(json_data)
        else:
            print("JSON not found in the data.")
            return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return None

def process_event(event):
    """
    Обработка входящего JSON-события.
    """
    if event:
        print("Received event:")
        print(json.dumps(event, indent=4, ensure_ascii=False))
        
        # Пример логики обработки
        scan_id = event.get("scan_id", "N/A")
        threat_level = event.get("result", {}).get("verdict", {}).get("threat_level", "UNKNOWN")
        
        # Пример действия
        if threat_level == "CLEAN":
            print(f"Scan {scan_id}: No threat detected.")
        elif threat_level == "MALICIOUS":
            print(f"Scan {scan_id}: Threat detected! Take immediate action.")
        else:
            print(f"Scan {scan_id}: Threat level unknown.")
    else:
        print("No valid event to process.")

def start_server():
    """
    Запуск TCP-сервера для обработки JSON-событий.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)  # Очередь на 5 подключений
        print(f"Server is listening on {HOST}:{PORT}...")

        try:
            while True:
                conn, addr = server_socket.accept()
                print(f"Connection established with {addr}")
                with conn:
                    data = conn.recv(4096).decode('utf-8')  # Читаем данные
                    if data:
                        # Проверяем наличие 'scan_machine.final_result' в заголовке
                        if 'scan_machine.final_result' in data:
                            event = extract_json(data)  # Извлекаем и парсим JSON
                            process_event(event)        # Обрабатываем событие
                            conn.sendall(b"Event processed\n")  # Ответ клиенту
                        else:
                            print("Event does not match the required header. Ignored.")
                            conn.sendall(b"Event ignored\n")  # Ответ клиенту
        except KeyboardInterrupt:
            print("\nServer is shutting down.")
        except Exception as e:
            print(f"Server error: {e}")

if __name__ == "__main__":
    start_server()