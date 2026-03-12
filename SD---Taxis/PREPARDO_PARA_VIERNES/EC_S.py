import socket
import sys
import threading
import select
import time


# Verificar los argumentos de entrada
if len(sys.argv) != 3:
    print("Usage: python3 EC_S.py <IP> <PORT EC_DE>")
    sys.exit(1)

server_ip = sys.argv[1]
server_port = int(sys.argv[2])

enviar_ok = True  # Inicializamos la variable enviar_ok

# Función para enviar mensajes
def enviar_mensaje(mensaje,client_socket):
    try:
        if client_socket:
            client_socket.send(mensaje.encode())
    except (BrokenPipeError,ConnectionResetError):
        print("El Taxi se ha destruido")
        client_socket.close()
        exit(1)

def send_message(client_socket):
    while True:
        if enviar_ok:
            enviar_mensaje("OK",client_socket)
        else:
            enviar_mensaje("KO",client_socket)
        time.sleep(1)

def connect_to_server(server_ip, server_port, client_port_base=7070):
    global enviar_ok  # Declaramos enviar_ok como global para poder modificarla

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

      # Espera de 1 segundo entre mensajes

    while True:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print(f"Cliente usando puerto {client_port_base} para conectarse...")
            # Intentar conectar al servidor
            
            
            client_socket.connect((server_ip, server_port))

            print(f"Conectado al servidor en {server_ip}:{server_port} desde el puerto {client_port_base}.")
        
            # Recibir el mensaje inicial del servidor
            message = client_socket.recv(1024)
            print(f"Mensaje del servidor: {message.decode()}")
            if message.decode() == "No permitido" or message.decode() == "Servidor detenido":
                print("El servidor ha enviado un mensaje de detención. Cerrando el cliente.")
                client_socket.close()
                exit(1)

            client_socket.settimeout(None)
            # Iniciar un hilo para enviar mensajes "OK" o "KO"
            threading.Thread(target=send_message , args=(client_socket,), daemon=True).start() 
            
            while True: 
                if select.select([sys.stdin], [], [], 0.1)[0]:
                    input()  # Consumir la entrada del teclado
                    enviar_ok = not enviar_ok  # Alternar entre enviar "OK" o "KO"
                    
        except socket.timeout:
            print(f"Conexión al servidor {server_ip}:{server_port} agotada después de 9 segundos.")
            exit(1)
        except ConnectionRefusedError:
            print(f"Conexión rechazada por el servidor {server_ip}:{server_port}.")
            client_socket.close()
            exit(1)
        except KeyboardInterrupt:
            print("El Sensor se ha desactivado correctamente")
            client_socket.sendall(b"exit")
            client_socket.close()
            exit(1)
        except ValueError:
            exit(1)
        finally:
            client_socket.close()

if __name__ == "__main__":
    connect_to_server(server_ip, server_port)