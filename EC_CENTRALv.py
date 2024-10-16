import socket
import sqlite3
import json
import threading
from colorama import Fore, Back, Style, init
from kafka import KafkaConsumer
from kafka import KafkaProducer
import numpy as np
import time
import os

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


producer_client_posicion=KafkaProducer(#Le indica al cliente donde se ha quedado
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
                                       

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
        cursor.execute(f"Update Taxi set estado = 'KO' where id = '{id}'")
        print(f"ID {id} inactivo.")
        conn.commit()
        os.system('clear')
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
            print(f"{id_taxi:<4} {posx:<5} {posy:<5} {estado:<5} {destino1:<8} {destino2:<8} {pasajero:<1}")
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
        threading.Thread(target=menu_central, args=(), daemon=True).start()
        while True:
            print("Esperando conexión...")
            client_socket, client_address = server_socket.accept()  # Aceptar conexión de cliente
            print(f"Cliente conectado desde: {client_address}")
            
            
           
            data = client_socket.recv(1024).decode().strip()  # Recibir mensaje del cliente  
            print(f"Mensaje: {data}")
            
            if len(data) > 1:
                diccionario = json.loads(data)
                print(diccionario)
                print("Es un diccionario")
                if diccionario.get('ESTADO') == "STOP":
                    borra_id(diccionario["id"])  # Usar el diccionario en lugar de 'data'
            else:
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
        producer_client.send('Servicios_cliente',value={"customer_id":"all","taxi_id":None})
        producer_client_posicion.send('Posicion_cliente',value={"customer_id":"all","cliente_pos":None})
        cambiar_estado_a_KO()
        time.sleep(0.5)
        exit(1)
    finally:
        server_socket.close()


def cambiar_estado_a_KO():
    conn=sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    cursor.execute(f"Update Taxi set estado = 'KO'")
    cursor.execute(f"Update Taxi set destino1 = NULL")
    cursor.execute(f"Update Taxi set destino2 = NULL")
    cursor.execute(f"Update Taxi set pasajero = NULL")
    cursor.execute(f"Delete from Cliente")
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
            destino1=cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0]
            destino2=cursor.execute(f"Select destino2 from Taxi where id = {message.value['id']}").fetchone()[0]
            if destino1 is not None:
                print(f"Servicio asignado {destino1}")
                posx=cursor.execute(f"Select posx from Cliente where id ={destino1}")
                posy=cursor.execute(f"Select posy from Cliente where id = {destino1}")
                producer_taxi.send('Servicio_taxi',value={"id":int(message.value['id']),"Estado":"RUN","destino":(int(posx),int(posy))})
                cursor.execute(f"Update Taxi set estado = 'RUN' where id = {message.value['id']}")
            elif destino2 is not None:
                print(f"Servicio asignado {destino2}")
                producer_taxi.send('Servicio_taxi',value={"id":int(message.value['id']),"Estado":"RUN","destino":ubicaciones[destino2]})
                cursor.execute(f"Update Taxi set estado = 'RUN' where id = {message.value['id']}")
            else:
                print("Taxi disponible")
                cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
            
            conn.commit()
            imprimir_base_datos()
        elif message.value["Estado"]== "END" and cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0] is not None:
            if cursor.execute(f"Select destino2 from Taxi where id = {message.value['id']}").fetchone()[0] is not None:
                pasajero=cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0]
                cursor.execute("Update Taxi set pasajero = ? where id = ?", (pasajero, message.value['id']))
                cursor.execute(f"Update Taxi set destino1 = NULL where id = {message.value['id']}")
                cursor.execute(f"Update Cliente set estado = 'OK{message.value['id']}' where id = '{pasajero}'")
                ubicacion=ubicaciones[cursor.execute(f"Select destino2 from Taxi where id = {message.value['id']}").fetchone()[0]]
                producer_taxi.send('Servicio_taxi',value={"id":int(message.value['id']),"Estado":"RUN","destino":ubicacion})
            else:
                cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
                cursor.execute(f"Update Taxi set destino1 = NULL where id = {message.value['id']}")
                
            conn.commit()
            imprimir_base_datos()
        elif message.value["Estado"]== "END" and cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0] is None:
            print("Servicio finalizado")
            pasajero=cursor.execute(f"Select pasajero from Taxi where id = {message.value['id']}").fetchone()[0]
            posx=cursor.execute(f"Select posx from Taxi where id = {message.value['id']}").fetchone()[0]
            posy=cursor.execute(f"Select posy from Taxi where id = {message.value['id']}").fetchone()[0]
            cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
            cursor.execute(f"Update Taxi set destino2 = NULL where id = {message.value['id']}")
            cursor.execute(f"Update Taxi set pasajero = NULL where id = {message.value['id']}")
            cursor.execute(f"Update Cliente set estado = 'OK' where id = '{pasajero}'")
            conn.commit()
            producer_client_posicion.send('Posicion_cliente',value={"customer_id":pasajero,"cliente_pos":(int(posx),int(posy))})
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

def imprimir_menu():
    print("A. Parar un taxi:")
    print("B. El taxi reanudará el servicio:")
    print("C. El taxi irá a un destino por el que se indica en esta petición:")
    print("D. Volver a la base:")
    


def menu_central():
    conn=sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    
    while True:
        imprimir_menu()
        opcion = input("Introduce una opción: ")
        if opcion == "A":#PARAR TAXI
            try:
                id_taxi = int(input("Introduce el ID del taxi: "))
                if cursor.execute(f"Select * from Taxi where id = {id_taxi}").fetchone() is not None:
                    if cursor.execute(f"Select estado from Taxi where id = {id_taxi}").fetchone()[0] != "KO":
                        cursor.execute(f"Update Taxi set estado = 'OKP' where id = {id_taxi}")
                        producer_taxi.send('DETENER_TAXI',value={"id":int(id_taxi),"Estado":"PARAR","destino":None})#Enviarlo a otro topic para que se pare de verdad
                        conn.commit()
                    else:
                        print("El taxi ya se encuentra parado.")
                else:
                    print("ID no existente.")        
            except ValueError:
                print("ID no válido.")
        elif opcion == "B":#REANUDAR TAXI
            try:
                id_taxi = int(input("Introduce el ID del taxi: "))
                if cursor.execute(f"Select * from Taxi where id = {id_taxi}").fetchone() is not None:
                    if cursor.execute(f"Select estado from Taxi where id = {id_taxi}").fetchone()[0] == "OKP":
                        destino1=cursor.execute(f"Select destino1 from Taxi where id = '{id_taxi}'").fetchone()[0]
                        destino2=cursor.execute(f"Select destino2 from Taxi where id = '{id_taxi}'").fetchone()[0]
                        if destino1 is not None:
                            print(f"Servicio reanudado en {destino1}")
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                            conn.commit()
                            producer_taxi.send('DETENER_TAXI',value={"id":int(id_taxi),"Estado":"REANUDAR"})
                            if destino1 == "Base":
                                producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN",
                                "destino":(1,1)})
                            else:
                                producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN",
                                "destino":(int(cursor.execute(f"Select posx from Cliente where id = '{destino1}'").fetchone()[0]),
                                int(cursor.execute(f"Select posy from Cliente where id = '{destino1}'").fetchone()[0]))})
                        elif destino2 is not None:
                            print(f"Servicio reanudado en {destino1}")
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                            conn.commit()
                            producer_taxi.send('DETENER_TAXI',value={"id":int(id_taxi),"Estado":"REANUDAR"})
                            producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN",
                            "destino":ubicaciones[destino2]})
                        else:
                            producer_taxi.send('DETENER_TAXI',value={"id":int(id_taxi),"Estado":"REANUDAR"})
                            cursor.execute(f"Update Taxi set estado = 'OK' where id = {id_taxi}")
                            conn.commit()
                    else:
                        print("El taxi ya se encuentra en servicio.")
                else:
                    print("ID no existente.")
            except ValueError:
                print("ID no válido.")  
        elif opcion == "C":#INDICAR UN DESTINO
            try:
                id_taxi = int(input("Introduce el ID del taxi: "))
                if cursor.execute(f"Select * from Taxi where id = {id_taxi}").fetchone() is not None:
                    if cursor.execute(f"Select estado from Taxi where id = {id_taxi}").fetchone()[0] == "OK":
                        destino = input("Introduce el nuevo destino: ")
                        if destino in ubicaciones.keys():
                            print(f"Destino cambiado a {destino}")
                            cursor.execute(f"Update Taxi set destino1 = '{destino}' where id = {id_taxi}")
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                            conn.commit()
                            producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN","destino":ubicaciones[destino]})
                        else:
                            print("Destino no válido") 
                    else:
                        print("El taxi no se encuentra en servicio.")
                else:
                    print("ID no existente.")
            except ValueError:
                print("ID no válido.")
        elif opcion == "D":
            try:
                id_taxi = int(input("Introduce el ID del taxi: "))
                if cursor.execute(f"Select * from Taxi where id = {id_taxi}").fetchone() is not None:
                    if cursor.execute(f"Select estado from Taxi where id = {id_taxi}").fetchone()[0] == "OK" or (cursor.execute(f"Select estado from Taxi where id = {id_taxi}").fetchone()[0] == "OKP" and cursor.execute(f"Select destino1 from Taxi where id = {id_taxi}").fetchone()[0] is None and cursor.execute(f"Select destino2 from Taxi where id = {id_taxi}").fetchone()[0] is None):
                        print(f"Taxi {id_taxi} volviendo a la base.")
                        cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                        cursor.execute(f"Update Taxi set destino1 = 'Base' where id = {id_taxi}")
                        producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN","destino":(1,1)})
                        conn.commit()
                    else:
                        print("El taxi no se encuentra en servicio.")
                else:
                    print("ID no existente.")
            except ValueError:
                print("ID no válido.")
        else:
            print("Opción no válida.")
        imprimir_base_datos()


def servicios_taxi():
    conn = sqlite3.connect('Taxi.db')
    
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Servicios'...")
    for message in consumer_servicios_cliente:
        print(f"Mensaje recibido: {message.value}")
        if message.value["destination"] is None:
            print(f"Cliente eliminado {message.value['customer_id']} no tiene mas servicios")
            cursor.execute(f"Delete from Cliente where id = '{message.value['customer_id']}'")
            conn.commit()
        else:
            taxi_disponible=seleccionar_taxi()
            if taxi_disponible:
                print(f"Taxi disponible ID:  {taxi_disponible}")
                if cursor.execute(f"Select * from Cliente where id = '{message.value['customer_id']}'").fetchone() is  None:
                    print("Cliente nuevo")
                    cursor.execute(f"Insert into Cliente (id, posx, posy, destino, estado) values ('{message.value['customer_id']}', '{message.value['pickup_location'][0]}', '{message.value['pickup_location'][1]}', '{message.value['destination']}', 'OK')")
                    conn.commit()
                else:
                    cursor.execute(f"Update Cliente set posx = {message.value['pickup_location'][0]} where id = '{message.value['customer_id']}'")
                    cursor.execute(f"Update Cliente set posy = {message.value['pickup_location'][1]} where id = '{message.value['customer_id']}'")
                    cursor.execute(f"Update Cliente set destino = '{message.value['destination']}' where id = '{message.value['customer_id']}'")
                    cursor.execute(f"Update Cliente set estado = 'OK' where id = '{message.value['customer_id']}'")
                cursor.execute(f"Update Taxi set destino1 = '{message.value['customer_id']}' where id = {taxi_disponible[0]}")
                cursor.execute(f"Update Taxi set estado = 'RUN' where id = {taxi_disponible[0]}")
                cursor.execute(f"Update Taxi set destino2 = '{message.value['destination']}' where id = {taxi_disponible[0]}")
                conn.commit()
                os.system('clear')
                imprimir_base_datos()
                producer_client.send('Servicios_cliente',value={"customer_id":message.value["customer_id"],"taxi_id":taxi_disponible[0]})
                producer_taxi.send('Servicio_taxi',value={"id":taxi_disponible[0],"Estado":"RUN","destino":message.value["pickup_location"]})
            else:
                print("No hay taxis disponibles.")
                time.sleep(0.5)
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
    ubicaciones["Base"]=(1,1)
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
