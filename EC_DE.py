import sys
import socket
import json
import time
import threading
import os
from colorama import Fore, Style
from kafka.errors import kafka_errors
from kafka import KafkaConsumer
from kafka import KafkaProducer



if len(sys.argv) != 6:
    print("Usage: python3 EC_DE.py <IP_Broker> <Puerto Broker> <IP CENTRAL> <CENTRAL PORT> <ID TAXI>")
    sys.exit(1)

ip_broker = sys.argv[1]
puerto_broker = sys.argv[2]
ip_central = sys.argv[3]
puerto_central = sys.argv[4]
id_taxi = sys.argv[5]

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
global en_movimiento
global ko
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
    producer_movs.send('Movs',value={"Movimiento":movimiento,"id":id_taxi})
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
    os.system('clear')  # Limpiar la consola
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
        print(f"Mensaje recibido: {message.value}")
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
    parar=False
    print("Esperando mensajes en el topic 'Servicio'...")
    consumer_servicio = KafkaConsumer(
    'Servicio_taxi',
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
    )
    for message in consumer_servicio:
        print(f"Mensaje recibido: {message.value}")
        if message.value["Estado"] == "STOP":#LA CENTRAL HA DESAPARECIDO
            print("Servidor detenido")
            client_socket.sendall(b"Servidor detenido")
            client_socket.close()
            exit(1)
        elif int(message.value["id"]) == int(id_taxi):
            print(f"Servicio asignado: {message.value}")
            en_movimiento=True
            parar=False
            mover_taxi(message.value["destino"])
            en_movimiento=False
            if ko==True or parar==True:
                print("NO ENVIO END")  
            else:
                print(f"Taxi {id_taxi} ha llegado a su destino en la posicion {taxipos}.")
                producer_estado.send('Estado', value={"Estado": "END", "id": id_taxi})
            
        
            
            







    


def handle_client(client_socket):
    global ko
    global en_movimiento
    try:
        client_socket.sendall(b"Conexion exitosa!")
        start_client(ip_central, puerto_central, id_taxi, client_socket,0)
        #threading.Thread(target=process_Mapa, args=(), daemon=True).start()
        ko=False
        
        threading.Thread(target=service_taxi, args=(client_socket,), daemon=True).start()
        threading.Thread(target=detener_taxi, args=(), daemon=True).start()
        threading.Thread(target=process_Mapa, args=(), daemon=True).start()
        while True:  # Bucle para manejar los mensajes del cliente
            data = client_socket.recv(1024)
            if not data:
                print("Cliente desconectado.")
                producer_estado.send('Estado',value={"Estado":"KO","id":id_taxi})
                break
            message = data.decode()
            
            if message == "KO" and ko==False:
                print("Enviado a central KO")
                producer_estado.send('Estado',value={"Estado":"KO","id":id_taxi})
                ko=True
            elif message == "OK" and ko==True:
                print("Enviado a central OK")
                producer_estado.send('Estado',value={"Estado":"OK","id":id_taxi})
                ko=False
    except OSError as e:
        print(f"Error al manejar el cliente:")
        exit(1)
    except KeyboardInterrupt:
        print("Servidor detenido.")
        if en_movimiento==True:
            producer_estado.send('Estado',value={"Estado":"STOP","id":id_taxi})
        client_socket.sendall(b"Servidor detenido")
        client_socket.close()
        start_client(ip_central, puerto_central, id_taxi, client_socket,1)
        exit(1)
    finally:
        client_socket.close()  # Cerrar la conexión del cliente


        
        
        
        
def start_client(ip_central, puerto_central, id_taxi, client_socket_client,error):
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        
        client_socket.connect((ip_central, int(puerto_central)))#CENTRAL
        if error == 1:
            value={"ESTADO":"STOP","id":id_taxi}
            client_socket.sendall(json.dumps(value).encode())
            client_socket.close()
            exit(1)
        threading.Thread(target=posiciones_taxi, args=(), daemon=True).start()
        time.sleep(1.5)
        client_socket.sendall(str(id_taxi).encode())
        
        if client_socket.recv(1024).decode() == "ID valido.":
            print("ID válido. Conexión establecida con el servidor central.")
            client_socket_client.sendall(b"ID valido.")
        else:
            print("Error: ID no válido no exite en la BD.")
            client_socket_client.sendall(b"Servidor detenido")
            client_socket_client.close()
            client_socket.close()
            exit(1)
    except ConnectionRefusedError:
        print("Error: No se pudo conectar al servidor central.")
        sys.exit(1)
    except KeyboardInterrupt:
            print(f"Servidor detenido manualmente.")
            value={"ESTADO":"STOP","id":id_taxi}
            client_socket.sendall(value.encode())
            client_socket.close()
            exit(1)
    




if __name__ == "__main__":

    start_server()
