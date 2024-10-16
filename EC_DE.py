import sys
import socket
import json
import time
import threading
from kafka.errors import kafka_errors
from kafka import KafkaConsumer
from kafka import KafkaProducer


producer_movs=KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_estado=KafkaProducer(     
    bootstrap_servers='localhost:9092',
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




consumer_mapa = KafkaConsumer(
    'Mapa',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='Taxis'
)

if len(sys.argv) != 4:
    print("Usage: python3 EC_DE.py <IP CENTRAL> <CENTRAL PORT> <ID TAXI>")
    sys.exit(1)

# Servidor empieza en 8080 y si está ocupado, intenta con el siguiente puerto
def start_server(base_port=8080):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = base_port

    while True:  # Este bucle es para seguir escuchando nuevos clientes
        try:
            server_socket.bind(('', port))
            server_socket.listen(1)
            print(f"Servidor escuchando en el puerto {port}...")
            
            while True:  # Bucle para aceptar múltiples clientes
                print("Esperando conexión...")
                client_socket, client_address = server_socket.accept()  # Aceptar la conexión del cliente
                client_ip, client_port = client_address
                
                print(f"Cliente conectado desde IP: {client_ip}, Puerto: {client_port}")

                # Manejar la comunicación con el cliente
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
        


def posiciones_taxi():
    global en_movimiento
    en_movimiento=False
    print("Esperando mensajes en el topic 'Posicion'...")
    consumer_posicion = KafkaConsumer(
    'Posicion',
    bootstrap_servers='localhost:9092',
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
    bootstrap_servers='localhost:9092',
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
    bootstrap_servers='localhost:9092',
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
        elif message.value["id"] == int(id_taxi):
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
        exit(1)
    except KeyboardInterrupt:
        print("Servidor detenido en la central.")
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
        
        client_socket.connect((ip_central, puerto_central))#CENTRAL
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

    puerto_central = int(sys.argv[2])
    ip_central = sys.argv[1]
    id_taxi = sys.argv[3]
    
    start_server()
