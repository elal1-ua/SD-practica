import socket
import sqlite3
import json
import threading
from colorama import Fore, Back, Style, init
from kafka import KafkaConsumer
from kafka import KafkaProducer
import numpy as np
import time
import requests 
import sys
import os



if len(sys.argv) != 4:
    print("Usage: python3 EC_CENTRAL.py <Puerto de Escucha> <IP del Broker> <Puerto del Broker>")
    sys.exit(1)

puerto=sys.argv[1]
ip_broker=sys.argv[2]
puerto_broker=sys.argv[3]

global ubicaciones
current_city = "Madrid"  # Ciudad predeterminada para EC_CTC
running_CTC = True # Variable para controlar el bucle principal del hilo de CTC
paused = False # Mapa no pausado por defecto
last_traffic_status = None # Último estado de tráfico

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

producer_client_posicion=KafkaProducer(#Le indica al cliente donde se ha quedado
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
                                       

producer_taxi_posicion=KafkaProducer(#Le indica al taxi donde se encuentra
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_client=KafkaProducer(#Le indica al cliente que tiene un taxi disponible
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_taxi=KafkaProducer(#Le indica al taxi que tiene un servicio disponible
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


consumer_servicios_cliente = KafkaConsumer(#Le llega el destino del cliente
    'Servicios',
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
)


consumer_taxi_estado = KafkaConsumer(#Actualiza el estado de los taxis
    'Estado',  # Nombre del topic
    bootstrap_servers=ip_broker+':'+puerto_broker,
    auto_offset_reset='latest',  
    enable_auto_commit=True,             
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
    )

consumer_taxi_movs = KafkaConsumer(#Actualiza la posición de los taxis
    'Movs',  # Nombre del topic
    bootstrap_servers=ip_broker+':'+puerto_broker,
    auto_offset_reset='latest',  
    enable_auto_commit=True,            
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

def check_traffic():  # Función para comprobar el tráfico en la ciudad actual
    global current_city
    global last_traffic_status
    EC_CTC_URL = os.getenv("TRAFFIC_API_URL", "http://localhost:8000/traffic-status")  # Endpoint configurable
    try:
        response = requests.get(EC_CTC_URL, params={"city": current_city})
        if response.status_code == 200:
            traffic_status = response.json()
            current_status = traffic_status['status'] # Estado actual del tráfico
            print(f"\n[INFO] Traffic status for {current_city}: {traffic_status['status']} - Reason: {traffic_status['reason']}")

            with sqlite3.connect('Taxi.db') as conn:  
                cursor = conn.cursor()

                if current_status == "KO" and last_traffic_status != "KO":
                    # Redirigir todos los taxis a la base
                    cursor.execute("SELECT * FROM Taxi WHERE posx != 1 OR posy != 1")
                    taxis_en_movimiento = cursor.fetchall()

                    for taxi in taxis_en_movimiento:
                        taxi_id, posx, posy, estado, destino1, destino2, pasajero = taxi
                        if estado == "RUN": # Está en movimiento, lo redirigimos a la base
                            cursor.execute(
                                "UPDATE Taxi SET destino1 = '1,1', destino2 = NULL, estado = 'REDIRECT' WHERE id = ?",
                                (taxi_id,),
                            )
                            producer_taxi.send('DETENER_TAXI',value={"id":taxi_id,"Estado":"PARAR","destino":None})
                            # Notificar al taxi vía Kafka
                            producer_taxi.send(
                                'Servicio_taxi',
                                value={"id": taxi_id, "Estado": "REDIRECT", "destino": (1, 1)},
                            )
                            print(f"[ACTION] Taxi {taxi_id} detenido y redirigido a la base (1,1).")
                        else: # está en OK  o similar
                            cursor.execute(
                                "UPDATE Taxi SET destino1 = '1,1', estado = 'REDIRECT' WHERE id = ?",
                                (taxi_id,),
                            )
                            print(f"[ACTION] Taxi {taxi_id} redirigido a la base (1,1).")
                            # Notificar al taxi vía Kafka
                            producer_taxi.send(
                                'Servicio_taxi',
                                value={"id": taxi_id, "Estado": "REDIRECT", "destino": (1,1)},
                            )

                        # Actualizar a los clientes que se han quedado colgados (sin taxi)
                        cursor.execute(
                            "UPDATE Cliente SET estado = 'KO' WHERE id NOT IN (SELECT pasajero FROM Taxi WHERE pasajero IS NOT NULL)"
                        )
                        cursor.execute("SELECT id FROM Cliente WHERE estado = 'KO'")
                        clientes_sin_taxi = cursor.fetchall()        
                        for cliente_id in clientes_sin_taxi:
                            print(f"[ACTION] Cliente {cliente_id} marcado como KO.")
                            # Notificar a los clientes vía Kafka
                            '''producer_client.send(
                                'Servicios_cliente',
                                value={"customer_id": cliente_id, "estado": "KO"}
                            )'''
                        

                        cursor.execute("UPDATE Cliente set estado = 'OKT' WHERE id IN (SELECT pasajero FROM Taxi WHERE pasajero IS NOT NULL)")
                        clientes_con_taxi = cursor.fetchall()
                        for cliente in clientes_con_taxi:
                            cliente_id, posx, posy, destino, estado = cliente
                            print(f"[ACTION] Cliente {cliente_id} esperando en el taxi a que se pueda volver a circular.")
                            


                elif current_status == "OK":
                    # Taxis sin pasajero
                    cursor.execute("SELECT * FROM Taxi WHERE estado = 'OKT' or estado = 'REDIRECT'")
                    taxis_en_base = cursor.fetchall()

                    # Clientes sin taxi asignado tienen que volver a pedir su servicio
                    cursor.execute("SELECT * FROM Cliente WHERE estado = 'KO'")
                    clientes_sin_taxi = cursor.fetchall()

                    # Seleccionar los clientes que tienen un taxi
                    cursor.execute("SELECT * FROM Cliente WHERE estado = 'OKT'")
                    clientes_con_taxi = cursor.fetchall()

                    for taxi in taxis_en_base:
                        taxi_id, posx, posy, estado, destino1, destino2, pasajero = taxi
                        cursor.execute(
                            "UPDATE Taxi SET estado = 'OK' WHERE id = ?",
                            (taxi_id,),
                        )
                        producer_taxi.send(
                            'Servicio_taxi',
                            value={"id": taxi_id, "Estado": "RESUME"},
                        )
                        if pasajero is None : # Venia sin ningun cliente 
                            print(f"[ACTION] Taxi {taxi_id} reanuda su servicio.")
                            
                        else :
                            print(f"[ACTION] Taxi {taxi_id} reanuda su servicio con el pasajero {pasajero}.")
                            

                    for cliente in clientes_sin_taxi:
                        cliente_id, posx, posy, destino, estado = cliente
                        "UPDATE Cliente SET estado = 'OK' WHERE estado = 'KO'"
                        print(f"[ACTION] Cliente {cliente_id} vuelve a solicitar un taxi.")

                        # Notificar al cliente vía Kafka
                        # Enviar solicitud de servicio al cliente
                        producer_client.send('Servicios', value={
                            "customer_id": cliente_id,
                            "pickup_location": (posx, posy),
                            "destination": destino
                        })
                    
                    for cliente in clientes_con_taxi:
                        cliente_id, posx, posy, destino, estado = cliente
                        taxi_id = cursor.execute("SELECT id FROM Taxi WHERE pasajero = ?", (cliente_id,)).fetchone()[0]
                        producer_client.send('Servicios', value={
                            "customer_id": cliente_id,
                            "pickup_location": (posx, posy),
                            "destination": destino
                        })
                        ''''producer_client.send('Servicios_cliente',value={"customer_id":cliente_id,"taxi_id":taxi_id})    
                        producer_taxi.send('Servicio_taxi', value={
                            "id": taxi_id,
                            "Estado": "RUN",
                            "destino": (posx, posy)
                        })'''
                        print(f"[ACTION] Cliente {cliente_id} vuelve a solicitar el servicio en el taxi {taxi_id}.")
            last_traffic_status = current_status
    except Exception as e:
        print("\n[ERROR] Imposible acceder al clima. Conexión con Openweather no disponible")
        print("[INFO] Reintentando en 5 segundos...")
        with sqlite3.connect('Taxi.db') as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE Taxi SET estado = 'OKT', destino1 = NULL, destino2 = NULL WHERE estado != 'KO'")
            cursor.execute("UPDATE Cliente SET estado = 'KO' WHERE id not in (SELECT pasajero FROM Taxi WHERE pasajero IS NOT NULL)")
            cursor.execute("UPDATE Cliente SET estado = 'OKT' WHERE id in (SELECT pasajero FROM Taxi WHERE pasajero IS NOT NULL)")
            cursor.execute("Select * From Taxi where estado = 'OKT'")
            taxis_detenidos = cursor.fetchall()
            for taxi in taxis_detenidos:
                taxi_id, posx, posy, estado, destino1, destino2, pasajero = taxi
                producer_taxi.send('DETENER_TAXI',value={"id":taxi_id,"Estado":"PARAR","destino":None})
        producer_taxi.send('Servicio_taxi',value={"Estado":"Weather_Error"})
        time.sleep(5)


def traffic_monitor():
    global running_CTC
    while running_CTC:
        check_traffic()
        time.sleep(10)  # Comprobar el tráfico cada 10 segundos

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
            imprimir()
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
        imprimir()
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
        print("\t\tTABLA TAXIS")
        print(f"{'ID':<2} {'POSX':<2} {'POSY':<2} {'ESTADO':<7} {'DESTINO1':<3} {'DESTINO2':<3} {'PASAJERO':<1}")
        print("-" * 60)
        
        # Imprimir cada registro
        for registro in registros:
            id_taxi, posx, posy, estado, destino1,destino2,pasajero = registro
            destino1 = destino1 if destino1 is not None else "Vacio"
            destino2 = destino2 if destino2 is not None else "Vacio"
            pasajero = pasajero if pasajero is not None else "Vacio"
            print(f"{id_taxi:<4} {posx:<5} {posy:<5} {estado:<5} {destino1:<8} {destino2:<8} {pasajero:<1}")
        print("-" * 60)
        
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
        threading.Thread(target=traffic_monitor, args=(), daemon=True).start()
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
            imprimir()
        elif message.value["Estado"] == "OK":
            destino1=cursor.execute(f"Select destino1 from Taxi where id = {message.value['id']}").fetchone()[0]
            destino2=cursor.execute(f"Select destino2 from Taxi where id = {message.value['id']}").fetchone()[0]
            
            if destino1 is not None:
                print(f"Servicio asignado {destino1}")
                
                if destino1 in ubicaciones.keys():
                    if destino1 == "Base":
                        print("Servicio asignado Base")
                        producer_taxi.send('Servicio_taxi',value={"id":message.value['id'],"Estado":"RUN","destino":(1,1)})
                    else:
                        print(f"Servicio asignado {destino1}")
                        producer_taxi.send('Servicio_taxi',value={"id":message.value['id'],"Estado":"RUN","destino":ubicaciones[destino1]})
                    cursor.execute(f"Update Taxi set estado = 'RUN' where id = {message.value['id']}")
                else:
                    posx=cursor.execute(f"Select posx from Cliente where id ='{destino1}'").fetchone()[0]
                    posy=cursor.execute(f"Select posy from Cliente where id = '{destino1}'").fetchone()[0]
                    print(f"posx: {posx} posy: {posy}")
                    producer_taxi.send('Servicio_taxi',value={"id":message.value['id'],"Estado":"RUN","destino":(int(posx),int(posy))})
                cursor.execute(f"Update Taxi set estado = 'RUN' where id = {message.value['id']}")
                
            elif destino2 is not None:
                print(f"Servicio asignado {destino2}")
                producer_taxi.send('Servicio_taxi',value={"id":int(message.value['id']),"Estado":"RUN","destino":ubicaciones[destino2]})
                cursor.execute(f"Update Taxi set estado = 'RUN' where id = {message.value['id']}")
            else:
                print("Taxi disponible")
                cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
            
            conn.commit()
            
            imprimir()
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
            imprimir()
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
            imprimir()
        elif message.value["Estado"] == "OKT":
            cursor.execute(f"Update Taxi set estado = 'OKT' where id = {message.value['id']}")
            cursor.execute(f"Update Taxi set destino1 = NULL where id = {message.value['id']}")
            conn.commit()
            imprimir()
        
        
        
def imprimir():
    global paused
    if paused : 
        return
    os.system('clear')
    ubicaciones_clientes, ubicaciones_taxi = leer_bbdd('Taxi.db')
    imprimir_base_datos()
    imprimir_base_datos_cliente()
    imprimir_mapa(crear_mapa(ubicaciones,ubicaciones_clientes,ubicaciones_taxi),ubicaciones_taxi)
        
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
    print("--- MENU CENTRAL ---")
    print("A. Parar un taxi:")
    print("B. El taxi reanudará el servicio:")
    print("C. El taxi irá a un destino por el que se indica en esta petición:")
    print("D. Volver a la base:")
    print("E. Cambiar ciudad para EC_CTC")


def menu_central():
    conn=sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    
    while True:
        paused = True
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
                            if destino1 in ubicaciones.keys():
                                if destino1 == "Base":
                                    producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN",
                                    "destino":(1,1)})
                                else:
                                    producer_taxi.send('Servicio_taxi',value={"id":int(id_taxi),"Estado":"RUN",
                                    "destino":ubicaciones[destino1]})
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
                    if cursor.execute(f"Select * from Taxi where id = {id_taxi} and estado ='OK'").fetchone()[0]:
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
        elif opcion == "D": # Volver a la base
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
        elif opcion == "E": # Cambiar ciudad
            global current_city
            new_city = input("Introduce el nombre de la ciudad: ")
            if new_city != current_city:
                current_city = new_city
                print(f"[INFO] Ciudad cambiada a {current_city}")
            else:
                print(f"Ya estás en la ciudad {current_city}")
        else:
            print("Opción no válida.")
        paused = False
        imprimir()
        


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
            imprimir()
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
                ubicaciones_clientes, ubicaciones_taxi = leer_bbdd('Taxi.db')
                imprimir_base_datos()
                imprimir_base_datos_cliente()
                imprimir_mapa(crear_mapa(ubicaciones,ubicaciones_clientes,ubicaciones_taxi),ubicaciones_taxi)
                time.sleep(0.5)
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
        if cursor.execute(f"Select pasajero from Taxi where id = {message.value['id']}").fetchone()[0] is not None:
            pasajero=cursor.execute(f"Select pasajero from Taxi where id = {message.value['id']}").fetchone()[0]
            cursor.execute(f"Update Cliente set posx = {posx} where id = '{pasajero}'")
            cursor.execute(f"Update Cliente set posy = {posy} where id = '{pasajero}'")
        cursor.execute(f"Update Taxi set posx = {posx}, posy = {posy} where id = {message.value['id']}")
        conn.commit()
        os.system('clear')
        ubicaciones_clientes, ubicaciones_taxi = leer_bbdd('Taxi.db')
        imprimir_base_datos()
        imprimir_base_datos_cliente()
        imprimir_mapa(crear_mapa(ubicaciones,ubicaciones_clientes,ubicaciones_taxi),ubicaciones_taxi)
        
    




'''
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

'''



def imprimir_base_datos_cliente():
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        
        # Obtener todos los registros de la tabla Cliente
        cursor.execute("SELECT * FROM Cliente")
        registros = cursor.fetchall()
        if registros == []:
            print("\t\tTABLA CLIENTES VACIA")
        else:
            print("\t\tTABLA CLIENTES")
            print(f"{'ID':<2} {'POSX':<10} {'POSY':<10} {'DESTINO':<10} {'ESTADO':<10}")
            print("-" * 60)
            for registro in registros:
                id_cliente, POSX, POSY, DESTINO, ESTADO = registro
                POSX = POSX if POSX is not None else "Vacio"
                POSY = POSY if POSY is not None else "Vacio"
                DESTINO = DESTINO if DESTINO is not None else "Vacio"
                ESTADO = ESTADO if ESTADO is not None else "Vacio"
                print(f"{id_cliente:<4} {POSX:<10} {POSY:<10} {DESTINO:<10} {ESTADO:<10}")
            print("-" * 60)
        print("\n")
    except sqlite3.Error as e:
        print(f"Error al intentar leer la base de datos: {e}")
    finally:
        conn.close()








def leer_bbdd(db_path):
    ubicaciones_clientes = {}  # Leo de bbdd y guardo ubis de clientes
    ubicaciones_taxi = {}  # Leo de bbdd y guardo ubis de taxis

    # Conexion a la base de datos
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Leo de clientes
    cursor.execute("SELECT * FROM Cliente")
    clientes = cursor.fetchall()  # Obtengo todos los registros de la tabla Cliente
    for cliente in clientes:
        id, posx, posy, destino, estado = cliente
        ubicaciones_clientes[id] = (int(posx), int(posy))  # Asegurar que posx y posy son enteros

    # Leo de taxis
    cursor.execute("SELECT * FROM Taxi")
    taxis = cursor.fetchall()  # Obtengo todos los registros de la tabla Taxi
    for taxi in taxis:
        id, posx, posy, estado, destino1, destino2, pasajero = taxi
        ubicaciones_taxi[id] = {'coordenadas': (int(posx), int(posy)), 'estado': estado, 'pasajero':pasajero}  # Asegurar enteros
    conn.close()
    return ubicaciones_clientes, ubicaciones_taxi


def leer_mapa(filename):
    ubicaciones = {}  # Diccionario para almacenar las ubicaciones

    with open(filename, 'r') as f:  # Leer el archivo línea por línea
        for linea in f:
            nombre, x, y = linea.strip().split(',')
            ubicaciones[nombre] = (int(x), int(y))  # Asegurar enteros
            
    ubicaciones["Base"] = (1, 1)  
    return ubicaciones


def crear_mapa(ubicaciones, ubicaciones_clientes, ubicaciones_taxi):
    # Crear un mapa vacío de 20x20 (inicializado con '-')
    mapa = np.full((20, 20), '-')

    # Insertar las ubicaciones en el mapa
    for nombre, (x, y) in ubicaciones.items():
        if nombre == "Base":
            continue
        if 1 <= x <= 20 and 1 <= y <= 20:  # Verificar que las coordenadas están dentro de los límites
            if mapa[x-1, y-1] != '-':  # Si ya hay un elemento en la celda
                mapa[x-1, y-1] = '*'  # Colocar un asterisco para indicar conflicto
            else:
                mapa[x-1, y-1] = nombre  # Insertar la ubicación
        else:
            print(f"Coordenadas fuera de límites para {nombre}: ({x},{y})")

    # Insertar los clientes
    for id_cliente, (x, y) in ubicaciones_clientes.items():
        if 1 <= x <= 20 and 1 <= y <= 20:
            if mapa[x-1, y-1] != '-':  # Si ya hay un elemento en la celda
                mapa[x-1, y-1] = '*'  # Colocar un asterisco para indicar conflicto
            else:
                mapa[x-1, y-1] = id_cliente.lower()  # Insertar cliente en minúscula
        else:
            print(f"Coordenadas fuera de límites para {id_cliente}: ({x},{y})")

    # Insertar los taxis
    for id_taxi, taxi_info in ubicaciones_taxi.items():
        x, y = taxi_info['coordenadas']
        if 1 <= x <= 20 and 1 <= y <= 20:
            if mapa[x-1, y-1] != '-':  # Si ya hay un elemento en la celda
                mapa[x-1, y-1] = '*'  # Colocar un asterisco para indicar conflicto
            else:
                mapa[x-1, y-1] = str(id_taxi)  # Insertar taxi
        else:
            print(f"Coordenadas fuera de límites para {id_taxi}: ({x},{y})")
    return mapa

def imprimir_mapa(mapa, ubicaciones_taxis):
    # Preparar los datos para imprimir
    tabla = []
    
    for i, fila in enumerate(mapa):
        fila_format = []
        for j, celda in enumerate(fila):
            conflicto_resuelto = False

            # Verificar si hay un taxi con pasajero en esta celda
            taxi_con_pasajero = None
            for taxi_id, taxi_info in ubicaciones_taxis.items():
                x, y = taxi_info['coordenadas']
                pasajero = taxi_info.get('pasajero', None)
                if x == i + 1 and y == j + 1 and pasajero:
                    taxi_con_pasajero = (taxi_id, pasajero)
                    break

            # Si encontramos un taxi con pasajero, verificamos si hay otro elemento
            if taxi_con_pasajero:
                taxi_id, pasajero = taxi_con_pasajero

                # Verificar si hay más de un elemento en esta celda
                # Si hay algún otro elemento (otro taxi, cliente, ubicación, etc.), mostramos el asterisco
                if mapa[i][j].isdigit() or mapa[i][j].islower() or mapa[i][j].isupper():
                    fila_format.append(Fore.CYAN + '*' + Style.RESET_ALL)  # Mostrar asterisco por conflicto
                else:
                    # Si solo hay el taxi con pasajero, mostrar el taxi con su pasajero
                    fila_format.append(Fore.GREEN + str(taxi_id) + pasajero.lower() + Style.RESET_ALL)
                conflicto_resuelto = True

            # Si no hay taxi con pasajero en conflicto, seguimos con la lógica normal
            if not conflicto_resuelto:
                if celda == '*':  # Si hay conflicto de elementos
                    fila_format.append(Fore.CYAN + '*' + Style.RESET_ALL)
                elif celda.isdigit():  # Si es un taxi, verificar su estado
                    taxi_id = int(celda)
                    taxi_info = ubicaciones_taxis.get(taxi_id, {})
                    taxi_estado = taxi_info.get('estado', 'KO')
                    pasajero = taxi_info.get('pasajero', None)
                    color = None

                    # Pintar en verde si está en movimiento (RUN), en rojo si está parado (KO), en blanco si está disponible (OK)
                    if taxi_estado == 'OK':
                        taxi_str = Fore.WHITE + celda + Style.RESET_ALL
                    elif taxi_estado == 'KO':
                        taxi_str = Fore.RED + celda + "!" + Style.RESET_ALL
                        color = 0
                    elif taxi_estado == 'OKP':
                        taxi_str = Fore.RED + celda + Style.RESET_ALL
                        color = 1
                    elif taxi_estado == 'RUN':
                        taxi_str = Fore.GREEN + celda + Style.RESET_ALL
                        color = 2
                    else:
                        taxi_str = celda  # Para taxis sin estado conocido

                    # Si el taxi tiene un pasajero, pintarlo igual que el taxi
                    if pasajero:
                        if color == 0:
                            taxi_str += Fore.RED + pasajero.lower() + Style.RESET_ALL
                        elif color == 1:
                            taxi_str += Fore.RED + pasajero.lower() + Style.RESET_ALL
                        else:
                            taxi_str += Fore.GREEN + pasajero.lower() + Style.RESET_ALL
                        fila_format.append(taxi_str)  # Imprimir taxi + pasajero
                    else:
                        fila_format.append(taxi_str)  # Solo el taxi si no tiene pasajero
                elif celda.islower():  # Si es un cliente, pintarlo en amarillo
                    fila_format.append(Fore.YELLOW + celda + Style.RESET_ALL)
                elif celda.isupper():  # Si es una ubicación, pintarla en azul
                    fila_format.append(Fore.BLUE + celda + Style.RESET_ALL)
                else:
                    fila_format.append(celda)  # Imprimir celdas vacías normales sin color
        tabla.append(fila_format)

    # Crear encabezado ajustado para los números del eje X
    encabezado = ["  "] + [f"{i:2}" for i in range(1, 21)]

    # Imprimir el encabezado con los números de columna
    print(" " + " ".join(encabezado))

    # Imprimir el mapa con los números de fila y celdas compactas
    for i, fila in enumerate(tabla):
        fila_str = "  ".join(fila)
        print(f"{i+1:2}  {fila_str}")

if __name__ == "__main__":
    
    ubicaciones=leer_mapa('mapa.txt')
    ubicaciones_clientes, ubicaciones_taxi = leer_bbdd('Taxi.db')
    imprimir_mapa(crear_mapa(ubicaciones,ubicaciones_clientes,ubicaciones_taxi),ubicaciones_taxi)
    imprimir_base_datos()
    imprimir_base_datos_cliente()
    start_server()