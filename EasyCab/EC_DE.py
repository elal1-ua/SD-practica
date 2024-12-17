import sys
import socket
import json
import time
import threading
import os
import ssl
import requests
from colorama import Fore, Style
from kafka import KafkaConsumer
from kafka import KafkaProducer
import urllib3
urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)


if len(sys.argv) != 6:
    print("Usage: python3 EC_DE.py <IP_Broker> <Puerto Broker> <IP CENTRAL> <CENTRAL PORT> <ID TAXI>")
    sys.exit(1)

ip_broker = sys.argv[1]
puerto_broker = sys.argv[2]
ip_central = sys.argv[3]
puerto_central = sys.argv[4]
id_taxi = sys.argv[5]
global token
producer_movs=KafkaProducer(
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_estado=KafkaProducer(     
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')    
)

global taxipos
global parar
global salir
global en_movimiento
global ko
global mata_hilo
global base 
salir=False
taxipos=[1,1]

DIRECTIONS = {
    "N": (-1, 0),
    "S": (1, 0),
    "W": (0, -1),
    "E": (0, 1),
    "NW": (-1, -1),
    "NE": (-1, 1),
    "SW": (1, -1),
    "SE": (1, 1)
}

cert_path='Registry.pem'
certificado_socket='cert.pem'
url = "https://localhost:3000/insert"
url_token = "https://localhost:3000/tokens"
data = {
    "id": id_taxi
}

def leer_token():
    global token
    try:
        response = requests.get(url_token,verify=cert_path)
        data = response.json()
    # Verificar la respuesta
        if response.status_code == 200:
            for item in data['result']:
                if item["id"]==id_taxi:
                    token=item["token"]
            
        else:
            print(f"Error: Código de estado {response.status_code}, Detalles: {response.text}")
            sys.exit(1)
            

    except requests.exceptions.RequestException as e:
        print("Error realizando la solicitud:", e)


def menu():
    print("1. Registrar taxi")
    print("2. Autenticar taxi")
    opcion = input("Seleccione una opción: ")
    return opcion

def registrar_taxi():
    try:
        
    # Haciendo la solicitud POST
        response = requests.post(url, json=data, verify=cert_path)  # Cambia verify a True si tienes un certificado válido

    # Verificar la respuesta
        if response.status_code == 200:
            print("Respuesta del servidor:", response.json())
            return True
        else:
            print(f"Error: Código de estado {response.status_code}, Detalles: {response.text}")
            sys.exit(1)
            return False

    except requests.exceptions.RequestException as e:
        print("Error realizando la solicitud:")


# Servidor empieza en 8080 y si está ocupado, intenta con el siguiente puerto
def start_server(base_port=8080):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = base_port

    while True:  # Este bucle es para seguir escuchando nuevos clientes
        try:
            server_socket.bind(('', port))
            server_socket.listen(1)
            print(f"Servidor escuchando en el puerto {port}...")
            print(f"Escuchando en la IP: {socket.gethostbyname(socket.gethostname())}")
            
            while True:  # Bucle para aceptar múltiples clientes
                print("Esperando conexión...")
                client_socket, client_address = server_socket.accept()  # Aceptar la conexión del cliente
                client_ip, client_port = client_address
                
                print(f"Cliente conectado desde IP: {client_ip}, Puerto: {client_port}")

                if client_socket:
                    while True:
                        opcion = menu()
                        if opcion == "1":
                            if registrar_taxi():
                                break
                            else:
                                exit(1)
                        elif opcion == "2":
                            break
                        else:
                            print("Opción no válida. Intente de nuevo.")
                    handle_client(client_socket)
                
        except OSError as e:
            print(f"Error al intentar iniciar el servidor en el puerto {port}: {e}")
            port += 1  # Intentar con el siguiente puerto    
        except KeyboardInterrupt:
            print("Servidor detenido manualmente.")
            server_socket.close()
            exit(1)





def calcular_mejor_direccion(destino):
    global taxipos
    
    delta_fila = int(destino[0]) - taxipos[0]
    delta_columna = int(destino[1]) - taxipos[1]
    
    if delta_fila < 0 and delta_columna < 0:
        movimiento="NW"
    elif delta_fila < 0 and delta_columna > 0:
        movimiento="NE"
    elif delta_fila > 0 and delta_columna < 0:
        movimiento="SW"
    elif delta_fila > 0 and delta_columna > 0:
        movimiento="SE"
    elif delta_fila < 0:
        movimiento="N"
    elif delta_fila > 0:
        movimiento="S"
    elif delta_columna < 0:
        movimiento="W"
    elif delta_columna > 0:
        movimiento="E"
    leer_token()
    producer_movs.send('Movs',value={"Movimiento":movimiento,"id":id_taxi,"token":token})
    time.sleep(2)
    return movimiento




def nueva_posicion(direccion):
    global taxipos
    movimiento = DIRECTIONS[direccion]
    nueva_fila = (taxipos[0] + movimiento[0]) % 20
    nueva_columna = (taxipos[1] + movimiento[1]) % 20
    
    if nueva_fila < 1:
        nueva_fila = 20
    elif nueva_fila > 20:
        nueva_fila = 1
    
    if nueva_columna < 1:
        nueva_columna = 20
    elif nueva_columna > 20:
        nueva_columna = 1
    
    return [nueva_fila, nueva_columna]



def mover_taxi(destino):
    global taxipos
    global parar
    global base
    while taxipos!=destino:
        if ko==True:
            print("KO")
            break
        if parar==True:
            print("Taxi detenido")
            break
        mejor_direccion=calcular_mejor_direccion(destino)
        taxipos=nueva_posicion(mejor_direccion)
        
            
        

def process_Mapa():
    consumer_mapa = KafkaConsumer(
    'Mapa',
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
    )
    for message in consumer_mapa:
        
        imprimir_mapa(crear_mapa(message.value['ubicaciones'],message.value['ubicaciones_cliente'],message.value['ubicaciones_taxi']))




def crear_mapa(ubicaciones, ubicaciones_clientes, ubicaciones_taxi):
    # Crear un mapa vacío de 20x20 (inicializado con listas vacías)
    mapa = [[[] for _ in range(20)] for _ in range(20)]
    no_imprimir_cliente = []
    # Insertar las ubicaciones en el mapa
    for nombre, (x, y) in ubicaciones.items():
        if nombre == "Base":
            continue
        if 1 <= x <= 20 and 1 <= y <= 20:  # Verificar que las coordenadas están dentro de los límites
            mapa[x-1][y-1].append(f"{Fore.BLUE}{nombre}{Style.RESET_ALL}")  # Añadir la ubicación en azul
        else:
            print(f"Coordenadas fuera de límites para {nombre}: ({x},{y})")

    # Insertar los clientes
    
    # Insertar los taxis
    for id_taxi, taxi_info in ubicaciones_taxi.items():
        x, y = taxi_info['coordenadas']
        if 1 <= x <= 20 and 1 <= y <= 20:
            if taxi_info['estado'] == 'ND':  # Estado 'ND' significa que no se muestra
                continue

            # Construir la representación del taxi
            taxi_str = f"{Fore.WHITE}{id_taxi}{Style.RESET_ALL}"  # Por defecto blanco
            if taxi_info['estado'] == 'KO':
                taxi_str = f"{Fore.RED}{id_taxi}{Style.RESET_ALL}{Fore.RED}{'!'}{Style.RESET_ALL}"
            elif taxi_info['estado'] == 'RUN':
                taxi_str = f"{Fore.GREEN}{id_taxi}{Style.RESET_ALL}"

            # Verificar si el taxi tiene un pasajero
            pasajero = taxi_info.get('pasajero')
            if pasajero:
                no_imprimir_cliente.append(pasajero)
                if taxi_info['estado'] == 'RUN':
                    taxi_str += f"({Fore.GREEN}{pasajero.lower()}{Style.RESET_ALL})"
                elif taxi_info['estado'] == 'OKP':
                    taxi_str += f"({Fore.RED}{pasajero.lower()}{Style.RESET_ALL})"
                elif taxi_info['estado'] == 'KO':
                    taxi_str += f"({Fore.RED}{pasajero.lower()}{Style.RESET_ALL})"

            mapa[x-1][y-1].append(taxi_str)  # Añadir taxi al mapa
        else:
            print(f"Coordenadas fuera de límites para {id_taxi}: ({x},{y})")
            
    for id_cliente, (x, y) in ubicaciones_clientes.items():
        if 1 <= x <= 20 and 1 <= y <= 20:
            if id_cliente in no_imprimir_cliente:
                continue
            else:
                mapa[x-1][y-1].append(f"{Fore.YELLOW}{id_cliente}{Style.RESET_ALL}")
        else:
            print(f"Coordenadas fuera de límites para {id_cliente}: ({x},{y})")

    return mapa


def imprimir_mapa(mapa):
    # Crear encabezado ajustado para los números del eje X
    encabezado = ["  "] + [f"{i:2}" for i in range(1, 21)]
     # Limpiar la consola
    # Imprimir el encabezado con los números de columna
    print(" " + " ".join(encabezado))

    # Imprimir el mapa con los números de fila y celdas compactas
    for i, fila in enumerate(mapa):
        fila_str = []
        for celda in fila:
            if len(celda) == 0:
                fila_str.append('-')  # Si la celda está vacía, mostrar '-'
            elif len(celda) == 1:
                fila_str.append(celda[0])  # Si hay solo una entidad, mostrarla directamente
            else:
                fila_str.append(f"[{','.join(celda)}]")  # Si hay varias entidades, mostrar el formato [A,6,l]
        
        print(f"{i+1:2}  {'  '.join(fila_str)}")












def posiciones_taxi():
    global en_movimiento
    en_movimiento=False
    print("Esperando mensajes en el topic 'Posicion'...")
    consumer_posicion = KafkaConsumer(
    'Posicion',
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
    )
     # Asegurar acceso exclusivo al consumidor
    for message in consumer_posicion:
        
        if message.value["id"] == id_taxi:
            taxipos[0] = message.value["posx"]
            taxipos[1] = message.value["posy"]
            print(f"Posicion actual: {taxipos}")
            exit(1)

def detener_taxi():
    global parar
    print("Esperando mensajes en el topic 'Posicion'...")
    consumer_detener = KafkaConsumer(
    'DETENER_TAXI',
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
    )
     # Asegurar acceso exclusivo al consumidor
    for message in consumer_detener:
        if message.value["id"] == int(id_taxi):
            if message.value["Estado"] == 'PARAR':
                print("Taxi ha sido detenido.")
                parar=True
            elif message.value["Estado"] == 'REANUDAR':
                print("Taxi ha sido reanudado.")
                parar=False



def service_taxi(client_socket):
    global en_movimiento
    global parar
    global mata_hilo
    global salir
    global ko
    parar=False
    if mata_hilo==True:#Cuando el taxi pierde el sensor y se vuelve a conectar otro se usa para que no existan multiples hilos de servicio
        exit(1)
    print("Esperando mensajes en el topic 'Servicio'...")
    consumer_servicio = KafkaConsumer(
    'Servicio_taxi',
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
    )
    for message in consumer_servicio:
        
        if message.value["Estado"] == "STOP":#LA CENTRAL HA DESAPARECIDO
            print("Servidor detenido")
            client_socket.sendall(b"Servidor detenido")
            client_socket.close()
            salir=True
            exit(1)
        elif message.value["Estado"] == "Weather_Error":
            print("\nError: OpenWeather no responde.")
            print("Esperando su reincorporación...")
            en_movimiento = False
            leer_token()
            producer_estado.send('Estado', value={"Estado": "OKT", "id": id_taxi,"token":token})
        elif int(message.value["id"]) == int(id_taxi):
            if message.value["Estado"] == "REDIRECT":
                print(f"Taxi {id_taxi} redirigido a la base (1,1).")
                en_movimiento = True
                parar = False # Para que no se detenga
                mover_taxi(message.value["destino"])
                en_movimiento = False  
                print(f"Taxi {id_taxi} ha llegado a la base.")
                leer_token()
                producer_estado.send('Estado', value={"Estado": "OKT", "id": id_taxi,"token":token}) # OKT = Redirect completado, esperando que se pueda volver a circular
            elif message.value["Estado"] == "RESUME":
                #if (message.value["destino"] == None):
                print(f"Taxi {id_taxi} vuelve a estar disponible.")
                leer_token()
                producer_estado.send('Estado', value={"Estado":"OK","id":id_taxi,"token":token})
            else:
                print(f"Servicio asignado: {message.value}")
                en_movimiento=True
                parar=False
                mover_taxi(message.value["destino"])
                en_movimiento=False
                if ko==True or parar==True:
                    print(f"Taxi {id_taxi} ha sido detenido.")
                else:
                    print(f"Taxi {id_taxi} ha llegado a su destino en la posicion {taxipos}.")
                    leer_token()
                    producer_estado.send('Estado', value={"Estado": "END", "id": id_taxi,"token":token})
                
        
            
            







    


def handle_client(client_socket):
    global ko
    global en_movimiento
    global salir
    global mata_hilo
    try:
        client_socket.sendall(b"Conexion exitosa!")
        
        start_client(ip_central, puerto_central, id_taxi, client_socket,0)
        threading.Thread(target=process_Mapa, args=(), daemon=True).start()
        ko=True
        threading.Thread(target=service_taxi, args=(client_socket,), daemon=True).start()
        threading.Thread(target=detener_taxi, args=(), daemon=True).start()
        threading.Thread(target=process_Mapa, args=(), daemon=True).start()
        while True:  # Bucle para manejar los mensajes del cliente
            data = client_socket.recv(1024)
            if salir==True:
                exit(1)
            if not data:
                print("Cliente desconectado.")
                leer_token()
                producer_estado.send('Estado',value={"Estado":"KO","id":id_taxi,"token":token})
                break
            message = data.decode()
            
            if message == "KO" and ko==False:
                print("Enviado a central KO")
                leer_token()
                producer_estado.send('Estado',value={"Estado":"KO","id":id_taxi,"token":token})
                ko=True
            elif message == "OK" and ko==True:
                print("Enviado a central OK")
                leer_token()
                producer_estado.send('Estado',value={"Estado":"OK","id":id_taxi,"token":token})
                ko=False
    except socket.error as e:
        print(f"Error de conexión: {e}")
        print("Error: Cliente desconectado.")
        if salir==True:
            exit(1)
        ko=True
        mata_hilo=True
        leer_token()
        producer_estado.send('Estado',value={"Estado":"KO","id":id_taxi,"token":token})
    except KeyboardInterrupt:
        print("Servidor detenido.")
        if en_movimiento==True:
            leer_token()
            producer_estado.send('Estado',value={"Estado":"STOP","id":id_taxi,"token":token})
        client_socket.sendall(b"Servidor detenido")
        client_socket.close()
        start_client(ip_central, puerto_central, id_taxi, client_socket,1)
        exit(1)
    finally:
        client_socket.close()  # Cerrar la conexión del cliente


        
        
        
        
def start_client(ip_central, puerto_central, id_taxi, client_socket_client,error):
    global token
    context=ssl._create_unverified_context()
    with socket.create_connection((ip_central, int(puerto_central))) as sock:
        with context.wrap_socket(sock,server_hostname=ip_central) as secure_client_socket:
            try:
                
                if error == 1:
                    value={"ESTADO":"STOP","id":id_taxi}
                    secure_client_socket.sendall(json.dumps(value).encode())
                    secure_client_socket.close()
                    exit(1)
                threading.Thread(target=posiciones_taxi, args=(), daemon=True).start()
                time.sleep(0.5)
                secure_client_socket.sendall(str(id_taxi).encode())
                data_decoded=secure_client_socket.recv(1024).decode()
                data_json=json.loads(data_decoded)
                if  data_json["message"]== "ID valido.":
                    print("ID válido. Conexión establecida con el servidor central.")
                    token=data_json["token"]
                    client_socket_client.sendall(b"ID valido.")
                else:
                    print("Error: ID no válido no exite en la BD.")
                    client_socket_client.sendall(b"Servidor detenido")
                    client_socket_client.close()
                    secure_client_socket.close()
                    sys.exit(1)
            except ConnectionRefusedError:
                print("Error: No se pudo conectar al servidor central.")
                sys.exit(1)
            except KeyboardInterrupt:
                print(f"Servidor detenido manualmente.")
                value={"ESTADO":"STOP","id":id_taxi}
                secure_client_socket.sendall(value.encode())
                secure_client_socket.close()
                sys.exit(1)
            except json.JSONDecodeError:
                print("EL ID NO ESTA AUTENTICADO.")
                sys.exit(1)

    




if __name__ == "__main__":
    mata_hilo=False
    base = False
    start_server()
