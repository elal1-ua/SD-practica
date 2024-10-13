import socket
import sqlite3
import json
import threading
from colorama import Fore, Back, Style, init
from kafka import KafkaConsumer
from kafka import KafkaProducer
import numpy as np
import time
import sys

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

global ubicaciones


producer_taxi_posicion=KafkaProducer(#Le indica al taxi donde se encuentra
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_client=KafkaProducer(#Le indica al cliente que tiene un taxi disponible
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_taxi=KafkaProducer(#Le indica al taxi que tiene un servicio disponible
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)





consumer_servicios_cliente = KafkaConsumer(#Le llega el destino del cliente
    'Servicios',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
)


consumer_taxi_estado = KafkaConsumer(#Actualiza el estado de los taxis
    'Estado',  # Nombre del topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  
    enable_auto_commit=True,             
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
    )

consumer_taxi_movs = KafkaConsumer(#Actualiza la posición de los taxis
    'Movs',  # Nombre del topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  
    enable_auto_commit=True,            
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
    )



def verificar_id(id):
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM Taxi WHERE id = {id}")
        if cursor.fetchone() is None:
            print(f"ID {id} no existente.")
            return False
        else:
            print(f"ID {id} ya existente.")
            cursor.execute(f"Update Taxi set estado = 'OK' where id = {id}")
            conn.commit()
            posx=cursor.execute(f"Select posx from Taxi where id = {id}").fetchone()[0]
            posy=cursor.execute(f"Select posy from Taxi where id = {id}").fetchone()[0]
            producer_taxi_posicion.send('Posicion',value={"id":id,"posx":posx,"posy":posy})
            imprimir_base_datos()
            return True
        
    except sqlite3.Error as e:
        print(f"Error al intentar verificar el ID: {e}")
        return False
    finally:
        conn.close()
    
    
def borra_id(id):
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        cursor.execute(f"Update Taxi set estado = 'KO' where id = {id}")
        print(f"ID {id} inactivo.")
        conn.commit()
        sys.clear()
        imprimir_base_datos()
    except sqlite3.Error as e:
        print(f"Error al intentar borrar el ID: {e}")
    finally:
        conn.close()

def imprimir_base_datos():
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        
        # Obtener todos los registros de la tabla Taxi
        cursor.execute("SELECT * FROM Taxi")
        registros = cursor.fetchall()
        
        print(f"{'ID':<2} {'POSX':<2} {'POSY':<2} {'ESTADO':<7} {'DESTINO1':<3} {'DESTINO2':<3} {'PASAJERO':<1}")
        print("-" * 60)
        
        # Imprimir cada registro
        for registro in registros:
            id_taxi, posx, posy, estado, destino1,destino2,pasajero = registro
            destino1 = destino1 if destino1 is not None else "Vacio"
            destino2 = destino2 if destino2 is not None else "Vacio"
            pasajero = pasajero if pasajero is not None else "Vacio"
            print(f"{id_taxi:<4} {posx:<5} {posy:<5} {estado:<5} {destino1:<3} {destino2:<4} {pasajero:<1}")
    except sqlite3.Error as e:
        print(f"Error al intentar leer la base de datos: {e}")
    finally:
        conn.close()


def start_server(base_port=3001):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = base_port

    try:
        server_socket.bind(('', port))
        server_socket.listen(10)  # Escuchar hasta 5 conexiones en cola
        print(f"Servidor escuchando en el puerto {port}...")
        threading.Thread(target=estado_taxi, args=(), daemon=True).start()
        threading.Thread(target=movimientos_taxi, args=(), daemon=True).start()
        threading.Thread(target=servicios_taxi, args=(), daemon=True).start()
        while True:
            print("Esperando conexión...")
            client_socket, client_address = server_socket.accept()  # Aceptar conexión de cliente
            print(f"Cliente conectado desde: {client_address}")
            
            
           
            data = client_socket.recv(1024).decode().strip()
            print(f"Mensaje: {data}")
            if isinstance(data, dict):
                if data.value["Estado"] == "STOP":
                    borra_id(data.value["id"])
            if verificar_id(data):
                print("ID valido.")
                client_socket.sendall(b"ID valido.")
            else:
                client_socket.sendall(b"Error: ID no existente.")  
            # Verificar si el ID existe en el fichero
            
            client_socket.close()  # Cerrar la conexión con el cliente

    except Exception as e:
        print(f"Error en el servidor: {e}")
        server_socket.close()
    except KeyboardInterrupt:
        print("Servidor detenido manualmente.")
        producer_taxi.send('Servicio_taxi',value={"Estado":"STOP"})
        cambiar_estado_a_KO()
        time.sleep(0.5)
        exit(1)
    finally:
        server_socket.close()


def cambiar_estado_a_KO():
    conn=sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    cursor.execute(f"Update Taxi set estado = 'KO'")
    conn.commit()

def estado_taxi():
    conn = sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Estado'...")
    for message in consumer_taxi_estado:
        print(f"Mensaje recibido: {message.value}")
        if message.value["Estado"] == "KO":
            cursor.execute(f"Update Taxi set estado = 'KO' where id = {message.value['id']}")
            conn.commit()
            imprimir_base_datos()
        elif message.value["Estado"] == "OK":
            cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
            conn.commit()
            imprimir_base_datos()
        elif message.value["Estado"]== "END" and cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0] is not None:
            cursor.execute(f"Update Taxi set destino1 = NULL where id = {message.value['id']}")
            conn.commit()
            ubicacion=ubicaciones[cursor.execute(f"Select destino2 from Taxi where id = {message.value['id']}").fetchone()[0]]
            producer_taxi.send('Servicio_taxi',value={"id":int(message.value['id']),"Estado":"RUN","destino":ubicacion})
            imprimir_base_datos()
        elif message.value["Estado"]== "END" and cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0] is None:
            print("Servicio finalizado")
            cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
            cursor.execute(f"Update Taxi set destino2 = NULL where id = {message.value['id']}")
            conn.commit()
            sys.clear()
            imprimir_base_datos()
            
def seleccionar_taxi():
    conn=sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    cursor.execute("SELECT * FROM Taxi WHERE estado = 'OK'")
    taxis = cursor.fetchall()
    if len(taxis) == 0:
        return None
    else:
        return taxis[0]            



def servicios_taxi():
    conn = sqlite3.connect('Taxi.db')
    
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Servicios'...")
    for message in consumer_servicios_cliente:
        print(f"Mensaje recibido: {message.value}")
        
        taxi_disponible=seleccionar_taxi()
        if taxi_disponible:
            print(f"Taxi disponible ID:  {taxi_disponible}")
            if cursor.execute(f"Select * from Cliente where id = '{message.value['customer_id']}'").fetchone() is  None:
                cursor.execute(f"Insert into Cliente (id, posx, posy, destino, estado) values ('{message.value['customer_id']}', '{message.value['pickup_location'][0]}', '{message.value['pickup_location'][1]}', '{message.value['destination']}', 'OK{taxi_disponible[0]}')")
            else:
                cursor.execute(f"Update Cliente set posx = {message.value['pickup_location'][0]} where id = '{message.value['customer_id']}'")
                cursor.execute(f"Update Cliente set posy = {message.value['pickup_location'][1]} where id = '{message.value['customer_id']}'")
                cursor.execute(f"Update Cliente set destino = '{message.value['destination']}' where id = '{message.value['customer_id']}'")
                cursor.execute(f"Update Cliente set estado = 'OK{taxi_disponible[0]}' where id = '{message.value['customer_id']}'")
            cursor.execute(f"Update Taxi set destino1 = '{message.value['customer_id']}' where id = {taxi_disponible[0]}")
            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {taxi_disponible[0]}")
            cursor.execute(f"Update Taxi set destino2 = '{message.value['destination']}' where id = {taxi_disponible[0]}")
            conn.commit()
            imprimir_base_datos()
            producer_client.send('Servicios_cliente',value={"customer_id":message.value["customer_id"],"taxi_id":taxi_disponible[0]})
            
            producer_taxi.send('Servicio_taxi',value={"id":taxi_disponible[0],"Estado":"RUN","destino":message.value["pickup_location"]})
        else:
            print("No hay taxis disponibles.")
            producer_client.send('Servicios_cliente',value={"customer_id":message.value["customer_id"],"taxi_id":None})
            
   
   
   


     
               
def movimientos_taxi():
    conn = sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Movs'...")
    for message in consumer_taxi_movs:
        print(f"Mensaje recibido: {message.value}")
        posx=cursor.execute(f"Select posx from Taxi where id = {message.value['id']}").fetchone()[0]
        posy=cursor.execute(f"Select posy from Taxi where id = {message.value['id']}").fetchone()[0]
        direc=DIRECTIONS[message.value["Movimiento"]]
        posx+=direc[0]
        posy+=direc[1]
        cursor.execute(f"Update Taxi set posx = {posx}, posy = {posy} where id = {message.value['id']}")
        conn.commit()
        imprimir_base_datos()
    





def leer_mapa(filename):
    ubicaciones = {}
    with open(filename, 'r') as f:
        for linea in f:
            nombre, x, y = linea.strip().split(',')
            ubicaciones[nombre] = (int(x), int(y))
    return ubicaciones

def crear_mapa(ubicaciones):
    # Crear un mapa vacío de 20x20 (inicializado con '-')
    mapa = np.full((20, 20), '-')

    # Insertar las ubicaciones en el mapa
    for nombre, (x, y) in ubicaciones.items():
        if 1 <= x <= 20 and 1 <= y <= 20:  # Verificar que las coordenadas están dentro de los límites
            mapa[x-1, y-1] = nombre  # Restar 1 ya que el índice en el array empieza en 0
        else:
            print(f"Coordenadas fuera de límites para {nombre}: ({x},{y})")
    
    return mapa

def imprimir_mapa(mapa):
    for fila in mapa:
        for celda in fila:
            if celda != '-':  # Si es una ubicación, pintarla en azul
                print(Fore.BLUE + celda + Style.RESET_ALL, end=' ')  # Pintar solo las letras en azul
            else:
                print(celda, end=' ')  # Imprimir celdas vacías normales sin color
        print() 







if __name__ == "__main__":
    
    ubicaciones=leer_mapa('mapa.txt')
    
    imprimir_mapa(crear_mapa(ubicaciones))
    imprimir_base_datos()
    start_server()
