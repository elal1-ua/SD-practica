import socket
import ssl
import random
import mysql.connector
from mysql.connector import Error
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
import jwt
import secrets
import datetime
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import urllib3
urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

def get_wifi_ip():
    try:
        # Conectarse a una dirección pública para determinar la inter>
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))  # Dirección IP de Google DNS
            wifi_ip = s.getsockname()[0]
        return wifi_ip
    except Exception as e:
        return f"Error obteniendo la IP: {e}"

cert_path='API_HTTPS.pem'
url="https://localhost:4000/send_message"

SECRET_KEY=secrets.token_hex(12)

# Genero claves RSA
private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
public_key = private_key.public_key()

with open("public_key.pem", "wb") as f:
    f.write(public_key.public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo
    ))

print("Clave pública generada y guardada en 'public_key.pem'.")

# Funciones de cifrado
def encrypt_message(aes_key, plaintext):
    iv = os.urandom(12)
    cipher = Cipher(algorithms.AES(aes_key), modes.GCM(iv))
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(plaintext.encode()) + encryptor.finalize()
    return iv + encryptor.tag + ciphertext

def decrypt_message(aes_key, encrypted_message):
    encrypted_message = bytes.fromhex(encrypted_message)
    iv, tag, ciphertext = encrypted_message[:12], encrypted_message[12:28], encrypted_message[28:]
    cipher = Cipher(algorithms.AES(aes_key), modes.GCM(iv, tag))
    decryptor = cipher.decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()

def generar_token(id):
    payload = {
        'id': id,
        'exp': datetime.datetime.now() + datetime.timedelta(hours=1)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')
    conn=create_connection()
    cursor=conn.cursor()
    cursor.execute("UPDATE Taxi SET token = %s WHERE id = %s",(token,id))
    conn.commit()
    print(f"Token generado para el taxi {id}")
    enviar_mensaje(f"Token generado para el taxi {id}")
    registro_de_mensajes(f"Token generado para el taxi {id}",ip_central)
    return token

def registro_de_mensajes(mensaje,ip,tipo="INFO"):
    
    conn=create_connection()
    cursor=conn.cursor()
    
    cursor.execute("INSERT INTO mensajes (mensaje,Ip_de_Origen,tipo) VALUES (%s,%s,%s)",(mensaje,ip,tipo))
    conn.commit()
    

if len(sys.argv) != 4:
    print("Usage: python3 EC_CENTRAL.py <Puerto de Escucha> <IP del Broker> <Puerto del Broker>")
    sys.exit(1)

CERTIFICATE_PATH = "cert.pem"
KEY_PATH = "key.pem"
puerto=sys.argv[1]
ip_broker=sys.argv[2]
puerto_broker=sys.argv[3]
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

global ip_taxis
global ip_cliente
global ip_central

global ubicaciones
current_city = "Madrid"  # Ciudad predeterminada para EC_CTC
running_CTC = True # Variable para controlar el bucle principal del hilo de CTC
paused = False # Mapa no pausado por defecto
last_traffic_status = None # Último estado de tráfico


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
            conn=create_connection()
            cursor = conn.cursor()
            if current_status == "KO" and last_traffic_status != "KO":
                # Redirigir todos los taxis a la base
                enviar_mensaje("Tráfico Interrumpido temperatura -0º. Redirigiendo taxis a la base.")
                registro_de_mensajes("Tráfico Interrumpido temperatura -0º. Redirigiendo taxis a la base.",ip_central)
                cursor.execute("SELECT * FROM Taxi WHERE posx != 1 OR posy != 1 or estado != 'KO'")
                taxis_en_movimiento = cursor.fetchall()
                for taxi in taxis_en_movimiento:
                    taxi_id, posx, posy, estado, destino1, destino2, pasajero,token,cifrado = taxi
                    if estado == "RUN": # Está en movimiento, lo redirigimos a la base
                        mensaje = {"id":taxi_id,"Estado":"PARAR","destino":None}
                        mensaje_json = json.dumps(mensaje)
                        encrypted_message = encrypt_message(bytes.fromhex(cifrado), mensaje_json)
                        producer_taxi.send('DETENER_TAXI',{"id":taxi_id,"data":encrypted_message.hex()})
                        # Notificar al taxi vía Kafka
                        mensaje = {"id":taxi_id,"Estado":"REDIRECT","destino":(1,1)}
                        mensaje_json = json.dumps(mensaje)
                        encrypted_message = encrypt_message(bytes.fromhex(cifrado), mensaje_json)
                        producer_taxi.send(
                            'Servicio_taxi',
                            {"id": taxi_id, "data": encrypted_message.hex()},
                        )
                        print(f"[ACTION] Taxi {taxi_id} detenido y redirigido a la base (1,1).")
                        enviar_mensaje(f"Taxi {taxi_id} detenido y redirigido a la base (1,1).")
                    else: # está en OK  o similar
                        cursor.execute(
                            "UPDATE Taxi SET destino1 = '1,1', estado = 'REDIRECT' WHERE id = %s",
                            (taxi_id,),
                        )
                        print(f"[ACTION] Taxi {taxi_id} redirigido a la base (1,1).")
                        enviar_mensaje(f"Taxi {taxi_id} redirigido a la base (1,1).")
                        # Notificar al taxi vía Kafka
                        mensaje = {"id":taxi_id,"Estado":"REDIRECT","destino":(1,1)}
                        mensaje_json = json.dumps(mensaje)
                        encrypted_message = encrypt_message(bytes.fromhex(cifrado), mensaje_json)
                        producer_taxi.send(
                            'Servicio_taxi',
                            {"id": taxi_id, "data": encrypted_message.hex()},
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
                    taxi_id, posx, posy, estado, destino1, destino2, pasajero,token,cifrado = taxi
                    cursor.execute(
                        "UPDATE Taxi SET estado = 'OK' WHERE id = %s",
                        (taxi_id,),
                    )
                    mensaje = {"id":taxi_id,"Estado":"RESUME"}
                    mensaje_json = json.dumps(mensaje)
                    encrypted_message = encrypt_message(bytes.fromhex(cifrado), mensaje_json)
                    producer_taxi.send(
                    'Servicio_taxi',
                    {"id": taxi_id, "data": encrypted_message.hex()},
                    )
                    if pasajero is None : # Venia sin ningun cliente 
                        print(f"[ACTION] Taxi {taxi_id} reanuda su servicio.")
                        enviar_mensaje(f"Taxi {taxi_id} reanuda su servicio.")
                        registro_de_mensajes(f"Taxi {taxi_id} reanuda su servicio.",ip_taxis)
                            
                    else :
                        print(f"[ACTION] Taxi {taxi_id} reanuda su servicio con el pasajero {pasajero}.")
                        enviar_mensaje(f"Taxi {taxi_id} reanuda su servicio con el pasajero {pasajero}.")
                        registro_de_mensajes(f"Taxi {taxi_id} reanuda su servicio con el pasajero {pasajero}.",ip_taxis)
                            
                for cliente in clientes_sin_taxi:
                    cliente_id, posx, posy, destino, estado = cliente
                    cursor.execute("UPDATE Cliente SET estado = 'OK' WHERE estado = 'KO'")
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
                    taxi_id = cursor.execute("SELECT id FROM Taxi WHERE pasajero = %s", (cliente_id,)).fetchone()[0]
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
                    enviar_mensaje(f"Cliente {cliente_id} vuelve a solicitar el servicio en el taxi {taxi_id}.")
        last_traffic_status = current_status
    except Exception as e:
        print(f"ERROR al acceder al estado del tráfico: {e}")
        print("\n[ERROR] Imposible acceder al clima. Conexión con Openweather no disponible")
        registro_de_mensajes("Imposible acceder al clima. Conexión con Openweather no disponible",ip_central,"ERROR")
        print("[INFO] Reintentando en 5 segundos...")
        with create_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE Taxi SET estado = 'OKT', destino1 = NULL, destino2 = NULL WHERE estado != 'KO'")
            cursor.execute("UPDATE Cliente SET estado = 'KO' WHERE id not in (SELECT pasajero FROM Taxi WHERE pasajero IS NOT NULL)")
            cursor.execute("UPDATE Cliente SET estado = 'OKT' WHERE id in (SELECT pasajero FROM Taxi WHERE pasajero IS NOT NULL)")
            cursor.execute("Select * From Taxi where estado = 'OKT'")
            taxis_detenidos = cursor.fetchall()
            for taxi in taxis_detenidos:
                taxi_id, posx, posy, estado, destino1, destino2, pasajero,token,cifrado = taxi
                mensaje = {"id":taxi_id,"Estado":"PARAR","destino":None}
                mensaje_json = json.dumps(mensaje)
                encrypted_message = encrypt_message(bytes.fromhex(cifrado), mensaje_json)
                producer_taxi.send('DETENER_TAXI',{"id":taxi_id,"data":encrypted_message.hex()})
        mensaje = {"Estado":"Weather_Error"}
        mensaje_json = json.dumps(mensaje)
        encrypted_message = encrypt_message(bytes.fromhex(cifrado), mensaje_json)
        producer_taxi.send('Servicio_taxi',{'id':taxi_id,'data':encrypted_message.hex()})
        time.sleep(5)
        
def traffic_monitor():
    global running_CTC
    while running_CTC:
        check_traffic()
        time.sleep(10)  # Comprobar el tráfico cada 10 segundos


def enviar_mensaje(mensaje):
    try:
        response = requests.post(url, json={"message":mensaje}, verify=cert_path)
        if response.status_code == 200:
            print("Mensaje enviado correctamente")
        else:
            print("Error al enviar mensaje")
    except requests.exceptions.RequestException as e:
        registro_de_mensajes(f"Error al enviar mensaje al API:",ip_central,"ERROR")
        print("Error realizando la solicitud:", e)

def create_connection():
    """Establece la conexión a la base de datos MySQL"""
    try:
        conn = mysql.connector.connect(
            host='localhost',    # Dirección del servidor MySQL
            user='enrique',            # Usuario de la base de datos
            password='passwd',     # Contraseña del usuario
            database='TAXI'                 # Puerto (usualmente 3306 para MySQL)
        )
        
        if conn.is_connected():
            
            return conn
        else:
            print('Error al conectar con la base de datos')
            return None

    except Error as e:
        registro_de_mensajes(f"Error de conexión: {e}",ip_central,"ERROR")
        print(f"Error de conexión: {e}")
        return None

def obtener_clave(id_taxi): 
    conn = create_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT cifrado FROM Taxi WHERE id = %s", (id_taxi,))
        return cursor.fetchone()[0]
    except mysql.connector.Error as e:
        print(f"Error al intentar obtener la clave: {e}")
        return None
    finally:
        conn.close()

producer_enviar_mapa=KafkaProducer(#Le indica al taxi toda la informacion del mapa
    bootstrap_servers=ip_broker+':'+puerto_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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



def verificar_id(id):
    try:
        conn = create_connection()
        cursor = conn.cursor()
        print(f"Verificando ID {id}...")
        cursor.execute(f"SELECT * FROM Taxi WHERE id = {id} and estado = 'ND'")
        if cursor.fetchone() is None:
            cursor.execute(f"SELECT * FROM Taxi WHERE id = {id}")
            if cursor.fetchone() is not None:
                print(f"ID {id} ya existente.")
                enviar_mensaje(f"ID {id} ya existente.")
                registro_de_mensajes(f"ID {id} ya existente.",ip_central)
                return False
            else:
                print(f"ID {id} no existente.")
                enviar_mensaje(f"ID {id} no existente.")
                return False
        else:
            print(f"ID {id} ya existente.")
            enviar_mensaje(f"ID {id} ya existente.")
            registro_de_mensajes(f"ID {id} ya existente.",ip_central,"ERROR")
            cursor.execute(f"UPDATE Taxi SET estado = 'OK' WHERE id = {id}")
            conn.commit()
            time.sleep(0.5)
            return True
        
    except Error as e:
        print(f"Error al intentar verificar el ID: {e}")
        registro_de_mensajes(f"Error al intentar verificar el ID:",ip_central,"ERROR")
        return False
    finally:
        conn.close()
    
    
def borra_id(id):
    try:
        
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE Taxi SET estado = 'ND' WHERE id = '{id}'")
        
        cursor.execute(f"SELECT destino2 FROM Taxi WHERE id = '{id}'")
        destino1 = cursor.fetchone()
        if destino1 is not None and destino1[0] is not None:  # Comprobar que destino1 no es None
            destino2_id = destino1[0]
            cursor.execute(f"UPDATE Cliente SET estado = 'OK' WHERE id = '{destino2_id}'")
            
            cursor.execute(f"SELECT posx,posy FROM Cliente WHERE id = '{destino2_id}'")
            posx, posy = cursor.fetchone()

            if posx is not None and posy is not None:  # Comprobar que no son None
                cursor.execute(f"UPDATE Cliente SET posx = {posx[0]} WHERE id = '{destino2_id}'")
                cursor.execute(f"UPDATE Cliente SET posy = {posy[0]} WHERE id = '{destino2_id}'")
                cursor.execute(f"UPDATE Cliente SET estado = 'OK' WHERE id = '{destino2_id}'")
                producer_client_posicion.send('Posicion_cliente', value={"customer_id": destino2_id, "cliente_pos": (int(posx[0]), int(posy[0]))})
        cursor.execute(f"SELECT pasajero FROM Taxi WHERE id = '{id}'")
        pasajero=cursor.fetchone()
        if pasajero is not None and pasajero[0] is not None:  # Comprobar que destino2 no es None
            pasajero_id = pasajero[0]
            cursor.execute(f"UPDATE Cliente SET estado = 'OK' WHERE id = '{pasajero_id}'")            
            cursor.execute(f"SELECT posx,posy FROM Cliente WHERE id = '{pasajero_id}'")
            posx,posy=cursor.fetchone()
            if posx is not None and posy is not None:  # Comprobar que no son None
                cursor.execute(f"UPDATE Cliente SET posx = {posx[0]} WHERE id = '{pasajero_id}'")
                cursor.execute(f"UPDATE Cliente SET posy = {posy[0]} WHERE id = '{pasajero_id}'")
                cursor.execute(f"UPDATE Cliente SET estado = 'OK' WHERE id = '{pasajero_id}'")
                producer_client_posicion.send('Posicion_cliente', value={"customer_id": pasajero_id, "cliente_pos": (int(posx[0]), int(posy[0]))})
        time.sleep(0.5)
        
        print(f"ID {id} inactivo.")
        enviar_mensaje(f"ID {id} inactivo.")
        registro_de_mensajes(f"ID {id} inactivo.",ip_taxis)
        cursor.execute(f"UPDATE Taxi SET destino1 = NULL WHERE id = '{id}'")
        cursor.execute(f"UPDATE Taxi SET destino2 = NULL WHERE id = '{id}'")
        cursor.execute(f"UPDATE Taxi SET pasajero = NULL WHERE id = '{id}'")
        cursor.execute(f"UPDATE Taxi SET token = NULL WHERE id = '{id}'")
        cursor.execute(f"UPDATE Taxi SET cifrado = NULL WHERE id = '{id}'")
        conn.commit()
        time.sleep(0.5)
        imprimir()
    except Error as e:
        print(f"Error al intentar borrar el ID: {e}")
        registro_de_mensajes(f"Error al intentar borrar el ID: {e}",ip_central,"ERROR")
    finally:
        conn.close()

def imprimir_base_datos():
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        # Obtener todos los registros de la tabla Taxi
        cursor.execute("SELECT * FROM Taxi")
        registros = cursor.fetchall()
        print("\t\tTABLA TAXIS")
        print(f"{'ID':<2} {'POSX':<2} {'POSY':<2} {'ESTADO':<7} {'DESTINO1':<3} {'DESTINO2':<3} {'PASAJERO':<1}")
        print("-" * 60)
        
        # Imprimir cada registro
        for registro in registros:
            id_taxi, posx, posy, estado, destino1,destino2,pasajero,token,cifrado = registro
            destino1 = destino1 if destino1 is not None else "Vacio"
            destino2 = destino2 if destino2 is not None else "Vacio"
            pasajero = pasajero if pasajero is not None else "Vacio"
            print(f"{id_taxi:<4} {posx:<5} {posy:<5} {estado:<5} {destino1:<8} {destino2:<8} {pasajero:<1}")
        print("-" * 60)
        
    except Error as e:
        print(f"Error al intentar leer la base de datos: {e}")
        registro_de_mensajes(f"Error al intentar leer la base de datos: ",ip_central,"ERROR")
    finally:
        conn.close()


def start_server(puerto):
    global ip_taxis
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = puerto
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(CERTIFICATE_PATH, KEY_PATH)
    with context.wrap_socket(server_socket, server_side=True) as secure_server_socket:
        try:
            secure_server_socket.bind(('', port))
            secure_server_socket.listen(10)  # Escuchar hasta 10 conexiones en cola
            print(f"Servidor escuchando en el puerto {port}...")
            registro_de_mensajes(f"Servidor CENTRAL escuchando en el puerto {port}...",ip_central)
            threading.Thread(target=estado_taxi, args=(), daemon=True).start()
            threading.Thread(target=movimientos_taxi, args=(), daemon=True).start()
            threading.Thread(target=servicios_taxi, args=(), daemon=True).start()
            threading.Thread(target=menu_central, args=(), daemon=True).start()
            threading.Thread(target=traffic_monitor, args=(), daemon=True).start()
            while True:
                print("Esperando conexión...")
                client_socket, client_address = secure_server_socket.accept()  # Aceptar conexión de cliente
                print(f"Cliente conectado desde: {client_address}")
                ip_taxis=client_address[0]
                registro_de_mensajes(f"Cliente conectado a CENTRAL desde: {client_address}",ip_taxis)
                try :
                    # Recibir clave AES cifrada
                    clave = b""
                    while True:
                        part = client_socket.recv(1024)
                        clave += part
                        if len(part) < 1024:  # Fin del mensaje
                            break
                    encrypted_aes_key = clave.decode() # Recibir clave del cliente
                    clave_aes = private_key.decrypt(
                        bytes.fromhex(encrypted_aes_key),
                        padding.OAEP(
                            mgf=padding.MGF1(algorithm=hashes.SHA256()),
                            algorithm=hashes.SHA256(),
                            label=None
                        )
                    )  
                    

                    data = client_socket.recv(1024).decode().strip()  # Recibir ID del cliente
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
                            enviar_mensaje(f"ID valido{data}.")
                            registro_de_mensajes(f"ID valido{data}.",ip_central)
                            token=generar_token(data)
                            data_to_send = {"message": "ID valido.","token":token}
                            client_socket.sendall(json.dumps(data_to_send).encode())
                            conn = create_connection()
                            cursor = conn.cursor()
                            cursor.execute(f"SELECT posx, posy FROM Taxi WHERE id = {data}")
                            posx,posy=cursor.fetchone()
                            cursor.execute(f"UPDATE Taxi SET cifrado = %s WHERE id = %s", (clave_aes.hex(), data))
                            print(f"Clave AES recibida y descifrada para el taxi {data}: {clave_aes.hex()}")
                            conn.commit()
                            conn.close()
                            print(f"[Servidor] Clave AES almacenada para el taxi {data}.")
                            time.sleep(1)
                            mensaje = {"id":data,"posx":posx,"posy":posy}
                            mensaje = json.dumps(mensaje)
                            encrypted_message = encrypt_message(clave_aes, mensaje)
                            producer_taxi_posicion.send('Posicion',{"id":data,"data":encrypted_message.hex()})
                            conn.commit()
                            conn.close()
                            imprimir()
                        else:
                            registro_de_mensajes(f"ID no valido{data}.",ip_central,"ERROR")
                            client_socket.sendall(b"Error: ID no existente.") 
                except Exception as e:
                    print(f"Error en el servidor: {e}")
                finally:
                    client_socket.close()

        except Exception as e:
            print(f"Error en el servidor: {e}")
            registro_de_mensajes(f"Error en el servidor CENTRAL: {e}",ip_central,"ERROR")
            secure_server_socket.close()
        except Error as e:
            print(f"Error al intentar conectar con la base de datos: {e}")
        except KeyboardInterrupt:
            print("Servidor detenido manualmente.")
            registro_de_mensajes("Servidor CENTRAL detenido manualmente.",ip_central)
            mensaje = {"Estado":"STOP"}
            mensaje_json = json.dumps(mensaje)
            encrypted_message = encrypt_message(clave_aes, mensaje_json)
            producer_taxi.send('Servicio_taxi',{"id":data,"data":encrypted_message.hex()})
            producer_client.send('Servicios_cliente',value={"customer_id":"all","taxi_id":None})
            producer_client_posicion.send('Posicion_cliente',value={"customer_id":"all","cliente_pos":None})
            cambiar_estado_a_ND()
            time.sleep(0.5)
            exit(1)
        finally:
            secure_server_socket.close()


def cambiar_estado_a_ND():
    conn=create_connection()
    cursor=conn.cursor()
    cursor.execute("Delete from Taxi")
    cursor.execute(f"Delete from Cliente")
    conn.commit()

def estado_taxi():
    conn = create_connection()
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Estado'...")
    for message in consumer_taxi_estado:
        taxi_id = message.value["id"]
        encrypted_data = message.value["data"]
        clave_aes = obtener_clave(taxi_id)
        if clave_aes is not None:
            decrypted_message = decrypt_message(bytes.fromhex(clave_aes), encrypted_data)
            mensaje_json = decrypted_message.decode()
            mensaje = json.loads(mensaje_json)
            print(f"Mensaje recibido {taxi_id} - {mensaje['Estado']}")
            cursor.execute(f"Select token from Taxi where id = {taxi_id}")
            token_bd=cursor.fetchone()
            
            if token_bd[0] == message.value['token']:
                cursor.execute(f"Select destino1 from Taxi where id = {taxi_id}")
                destino_1=cursor.fetchone()
                if mensaje["Estado"] == "KO":
                    print(f"Taxi {taxi_id} fuera de servicio")
                    enviar_mensaje(f"Taxi {taxi_id} fuera de servicio(KO)")
                    registro_de_mensajes(f"Taxi {taxi_id} fuera de servicio(KO)")
                    cursor.execute(f"Update Taxi set estado = 'KO' where id = {taxi_id}")
                    conn.commit()
                    imprimir()
                elif mensaje["Estado"] == "OK":
                    cursor.execute(f"Select destino1 from Taxi where id = {taxi_id}")
                    destino1=cursor.fetchone()[0]
                    cursor.execute(f"Select destino2 from Taxi where id = {taxi_id}")
                    destino2=cursor.fetchone()[0]
                    if destino1 is not None:
                        if destino1 in ubicaciones.keys():
                            if destino1 == "Base":
                                print("Servicio asignado Base")
                                enviar_mensaje(f"Servicio asignado Base a taxi {taxi_id}")
                                registro_de_mensajes(f"Servicio asignado Base a taxi {taxi_id}",ip_taxis)
                                mensaje_prod = {"id":taxi_id,"Estado":"RUN","destino":(1,1)}
                                mensaje_prod_json = json.dumps(mensaje_prod)
                                encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_prod_json)
                                producer_taxi.send('Servicio_taxi',{"id":taxi_id,"data":encrypted_message.hex()})
                            else:
                                print(f"Servicio asignado {destino1}")
                                mensaje_prod = {"id":taxi_id,"Estado":"RUN","destino":ubicaciones[destino1]}
                                mensaje_prod_json = json.dumps(mensaje_prod)
                                encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_prod_json)
                                producer_taxi.send('Servicio_taxi',{"id":taxi_id,"data":encrypted_message.hex()})
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {taxi_id}")
                        else:
                            cursor.execute(f"Select posx,posy from Cliente where id ='{destino1}'")
                            posx,posy=cursor.fetchone()
                            
                            mensaje_prod = {"id":taxi_id,"Estado":"RUN","destino":(int(posx),int(posy))}
                            mensaje_prod_json = json.dumps(mensaje_prod)
                            encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_prod_json)
                            producer_taxi.send('Servicio_taxi',{"id":taxi_id,"data":encrypted_message.hex()})
                        cursor.execute(f"Update Taxi set estado = 'RUN' where id = {taxi_id}")   
                    elif destino2 is not None:
                        print(f"Servicio asignado {destino2}")
                        enviar_mensaje(f"Servicio asignado {destino2} a taxi {taxi_id}")
                        registro_de_mensajes(f"Servicio asignado {destino2} a taxi {taxi_id}",ip_taxis)
                        mensaje_prod = {"id":taxi_id,"Estado":"RUN","destino":ubicaciones[destino2]}
                        mensaje_prod_json = json.dumps(mensaje_prod)
                        encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_prod_json)
                        producer_taxi.send('Servicio_taxi',{"id":int(taxi_id),"data":encrypted_message.hex()})
                        cursor.execute(f"Update Taxi set estado = 'RUN' where id = {taxi_id}")
                    else:
                        print("Taxi disponible")
                        cursor.execute(f"Update Taxi set estado = 'OK' where id = {taxi_id}")
                    
                    conn.commit()
                        
                    imprimir()
                elif mensaje["Estado"]== "END" and destino_1[0] is not None:
                    cursor.execute(f"Select posx,posy from Taxi where id = {message.value['id']}")
                    posx,posy=cursor.fetchone()

                    cursor.execute(f"Select destino2 from Taxi where id = {taxi_id}")
                    destino2=cursor.fetchone()[0]
                    if destino2 is not None:
                        cursor.execute(f"Select destino1 from Taxi where id = {taxi_id}")
                        pasajero=cursor.fetchone()[0]
                        print(f"Servicio finalizado {taxi_id}")
                        cursor.execute("Update Taxi set pasajero = %s where id = %s", (pasajero, taxi_id))
                        cursor.execute(f"Update Taxi set destino1 = NULL where id = {taxi_id}")
                        cursor.execute(f"Update Cliente set estado = 'RUN' where id = '{pasajero}'")
                            
                        ubicacion=ubicaciones[destino2]
                        mensaje_prod = {"id":taxi_id,"Estado":"RUN","destino":ubicacion}
                        mensaje_prod_json = json.dumps(mensaje_prod)
                        encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_prod_json)
                        producer_taxi.send('Servicio_taxi',{"id":int(taxi_id),"data":encrypted_message.hex()})
                    else:
                        cursor.execute(f"Update Taxi set estado = 'OK' where id = {taxi_id}")
                        cursor.execute(f"Update Taxi set destino1 = NULL where id = {taxi_id}")
                            
                    conn.commit()
                    if posx==1 and posy==1:
                        generar_token(message.value['id'])
                    imprimir()
                elif mensaje["Estado"]== "END" and destino_1[0] is None:
                    cursor.execute(f"Select posx,posy from Taxi where id = {message.value['id']}")
                    posx,posy=cursor.fetchone()

                    print(f"Servicio finalizado {taxi_id}")
                    enviar_mensaje(f"Servicio finalizado {taxi_id}")
                    registro_de_mensajes(f"Servicio finalizado {taxi_id}", ip_taxis)
                    time.sleep(0.5)
                    cursor.execute(f"Select pasajero from Taxi where id = {taxi_id}")
                    pasajero=cursor.fetchone()[0]
                    cursor.execute(f"Select posx,posy from Taxi where id = {taxi_id}")
                    posx,posy=cursor.fetchone()
                    cursor.execute(f"Update Taxi set estado = 'OK' where id = {taxi_id}")
                    cursor.execute(f"Update Taxi set destino2 = NULL where id = {taxi_id}")
                    cursor.execute(f"Update Taxi set pasajero = NULL where id = {taxi_id}")
                    cursor.execute(f"Update Cliente set estado = 'OK' where id = '{pasajero}'")
                    conn.commit()
                    producer_client_posicion.send('Posicion_cliente',value={"customer_id":pasajero,"cliente_pos":(int(posx),int(posy))})
                    if posx==1 and posy==1:
                        generar_token(message.value['id'])
                    imprimir()
                elif mensaje["Estado"] == "OKT":
                    registro_de_mensajes(f"Hace mucho frio, taxi {taxi_id} fuera de servicio",ip_taxis)
                    cursor.execute(f"Update Taxi set estado = 'OKT' where id = {taxi_id}")
                    cursor.execute(f"Select pox,posy from Taxi where id = {taxi_id}")
                    posx,posy = cursor.fetchone()
                    conn.commit()
                    if posx==1 and posy==1:
                        generar_token(message.value['id'])
                    imprimir()
                else:
                    print(f"Token incorrecto para taxi {taxi_id}")
                    enviar_mensaje(f"Token incorrecto para taxi {taxi_id}")
                    registro_de_mensajes(f"Token incorrecto para taxi {taxi_id}",ip_central,"ERROR")
            
        else :
            print("No se encontró la clave para el taxi {taxi_id}")
        
        
def imprimir():
    num=random.randint(1, 100)
    if paused:
        return
    ubicaciones_clientes, ubicaciones_taxi = leer_bbdd('Taxi.db')
    imprimir_base_datos()
    imprimir_base_datos_cliente()
    imprimir_mapa(crear_mapa(ubicaciones,ubicaciones_clientes,ubicaciones_taxi))
    enviar_mapa()
        
def seleccionar_taxi():
    conn=create_connection()
    cursor=conn.cursor()
    cursor.execute("SELECT * FROM Taxi WHERE estado = 'OK'")
    taxis = cursor.fetchall()
    if len(taxis) == 0:
        return None
    else:
        return taxis[0]            

def imprimir_menu():
    print("---MENU CENTRAL---")
    print("A. Parar un taxi:")
    print("B. El taxi reanudará el servicio:")
    print("C. El taxi irá a un destino por el que se indica en esta petición:")
    print("D. Volver a la base:")
    print("E.Cambiar ciudad para EC_CTC:")
    


def menu_central():
    global ip_central
    global paused
    conn=create_connection()
    cursor=conn.cursor()
    
    while True:
        paused = True
        imprimir_menu()
        opcion = input("Introduce una opción: ")
        if opcion == "A":#PARAR TAXI
            try:
                id_taxi = int(input("Introduce el ID del taxi: "))
                
                if cursor.execute(f"Select * from Taxi where id = {id_taxi}").fetchone() is not None:
                    cursor.execute(f"Select estado from Taxi where id = {id_taxi}")
                    estado=cursor.fetchone()[0]
                    if estado != "KO":
                        cursor.execute(f"Update Taxi set estado = 'OKP' where id = {id_taxi}")
                        mensaje = {"id":id_taxi,"Estado":"PARAR", "destino" : None}
                        mensaje_json = json.dumps(mensaje)
                        clave_aes = obtener_clave(id_taxi)
                        encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                        producer_taxi.send('DETENER_TAXI',{"id": int(id_taxi), "data":encrypted_message.hex() })#Enviarlo a otro topic para que se pare de verdad
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
                cursor.execute(f"Select * from Taxi where id = {id_taxi}")
                total=cursor.fetchone()
                if total is not None:
                    cursor.execute(f"Select estado from Taxi where id = {id_taxi}")
                    estado=cursor.fetchone()[0]
                    if estado == "OKP":
                        cursor.execute(f"Select destino1 from Taxi where id = '{id_taxi}'")
                        destino1=cursor.fetchone()[0]
                        cursor.execute(f"Select destino2 from Taxi where id = '{id_taxi}'")
                        destino2=cursor.fetchone()[0]
                        if destino1 is not None:
                            print(f"Servicio reanudado en {destino1}")
                            enviar_mensaje(f"Taxi {id_taxi} servicio reanudado en {destino1}")
                            registro_de_mensajes(f"Taxi {id_taxi} servicio reanudado en {destino1}",ip_taxis)
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                            conn.commit()
                            mensaje = {"id":id_taxi,"Estado":"REANUDAR"}
                            mensaje_json = json.dumps(mensaje)
                            clave_aes = obtener_clave(id_taxi)
                            encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                            producer_taxi.send('DETENER_TAXI',{"id": int(id_taxi), "data":encrypted_message.hex() })
                            if destino1 in ubicaciones.keys():
                                if destino1 == "Base":
                                    mensaje = {"id":int(id_taxi),"Estado":"RUN","destino":(1,1)}
                                    mensaje_json = json.dumps(mensaje)
                                    encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                                    producer_taxi.send('Servicio_taxi',{"id":int(id_taxi),"data":encrypted_message.hex()})
                                else:
                                    mensaje = {"id":int(id_taxi),"Estado":"RUN","destino":ubicaciones[destino1]}
                                    mensaje_json = json.dumps(mensaje)
                                    encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                                    producer_taxi.send('Servicio_taxi',{"id":int(id_taxi),"data":encrypted_message.hex()})
                            else:
                                cursor.execute(f"Select posx from Cliente where id = '{destino1}'")
                                posx=cursor.fetchone()[0]
                                cursor.execute(f"Select posy from Cliente where id = '{destino1}'")
                                posy=cursor.fetchone()[0]
                                mensaje = {"id":int(id_taxi),"Estado":"RUN","destino":(int(posx),int(posy))}
                                mensaje_json = json.dumps(mensaje)
                                encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                                producer_taxi.send('Servicio_taxi',{"id":int(id_taxi),"data":encrypted_message.hex()})
                        elif destino2 is not None:
                            print(f"Servicio reanudado en {destino1}")
                            enviar_mensaje(f"Taxi {id_taxi} servicio reanudado en {destino2}")
                            registro_de_mensajes(f"Taxi {id_taxi} servicio reanudado en {destino2}",ip_taxis)
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                            conn.commit()

                            mensaje = {"id":int(id_taxi),"Estado":"REANUDAR"}
                            mensaje_json = json.dumps(mensaje)
                            clave_aes = obtener_clave(id_taxi)
                            encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                            producer_taxi.send('DETENER_TAXI',{"id": int(id_taxi), "data":encrypted_message.hex() })

                            mensaje = {"id":int(id_taxi),"Estado":"RUN","destino":ubicaciones[destino2]}
                            mensaje_json = json.dumps(mensaje)
                            encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                            producer_taxi.send('Servicio_taxi',{"id":int(id_taxi),"data":encrypted_message.hex()})
                        else:
                            mensaje = {"id":int(id_taxi),"Estado":"REANUDAR"}
                            mensaje_json = json.dumps(mensaje)
                            clave_aes = obtener_clave(id_taxi)
                            encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                            producer_taxi.send('DETENER_TAXI',{"id": int(id_taxi), "data":encrypted_message.hex() })
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
                cursor.execute(f"Select * from Taxi where id = {id_taxi}")
                total=cursor.fetchone()
                if total is not None:
                    cursor.execute(f"Select * from Taxi where id = {id_taxi} and estado ='OK'")
                    total1=cursor.fetchone()
                    if total1 is not None:
                        destino = input("Introduce el nuevo destino: ")
                        if destino in ubicaciones.keys():
                            print(f"Destino cambiado a {destino}")
                            enviar_mensaje(f"El Destino de {id_taxi} cambiado a {destino}")
                            registro_de_mensajes(f"El Destino de {id_taxi} cambiado a {destino}",ip_taxis)
                            cursor.execute(f"Update Taxi set destino1 = '{destino}' where id = {id_taxi}")
                            cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                            conn.commit()
                            mensaje = {"id":int(id_taxi),"Estado":"RUN","destino":ubicaciones[destino]}
                            mensaje_json = json.dumps(mensaje)
                            clave_aes = obtener_clave(id_taxi)
                            encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                            producer_taxi.send('Servicio_taxi',{"id":int(id_taxi),"data":encrypted_message.hex()})
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
                cursor.execute(f"Select * from Taxi where id = {id_taxi}")
                if cursor.fetchone() is not None:
                    cursor.execute(f"Select estado from Taxi where id = {id_taxi}")
                    estado=cursor.fetchone()[0]
                    cursor.execute(f"Select destino1 from Taxi where id = {id_taxi}")
                    destino1=cursor.fetchone()[0]
                    cursor.execute(f"Select destino2 from Taxi where id = {id_taxi}")
                    destino2=cursor.fetchone()[0]
                    if estado== "OK" or estado == "OKP" and (destino1 is None and destino2 is None):
                        print(f"Taxi {id_taxi} volviendo a la base.")
                        cursor.execute(f"Update Taxi set estado = 'RUN' where id = {id_taxi}")
                        cursor.execute(f"Update Taxi set destino1 = 'Base' where id = {id_taxi}")
                        mensaje = {"id":int(id_taxi),"Estado":"RUN","destino":(1,1)}
                        mensaje_json = json.dumps(mensaje)
                        clave_aes = obtener_clave(id_taxi)
                        encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_json)
                        producer_taxi.send('Servicio_taxi',{"id":int(id_taxi),"data":encrypted_message.hex()})
                        conn.commit()
                    else:
                        print("El taxi no se encuentra en servicio.")
                else:
                    print("ID no existente.")
            except ValueError:
                print("ID no válido.")
        elif opcion == "E":
            global current_city
            new_city = input("Introduce el nombre de la ciudad: ")
            if new_city != current_city:
                current_city = new_city
                print(f"[INFO] Ciudad cambiada a {current_city}")
                enviar_mensaje(f"Ciudad cambiada a {current_city}")
                registro_de_mensajes(f"Ciudad cambiada a {current_city}",ip_central)
            else:
                print(f"Ya estás en la ciudad {current_city}")
        else:
            print("Opción no válida.")
        paused = False
        imprimir()
        


def servicios_taxi():
    global ip_cliente
    conn = create_connection()
    
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Servicios'...")
    for message in consumer_servicios_cliente:
        print(f"Mensaje recibido: {message.value}")
        if message.value["destination"] is None:
            print(f"Cliente eliminado {message.value['customer_id']} no tiene mas servicios")
            enviar_mensaje(f"Cliente eliminado {message.value['customer_id']} no tiene mas servicios")
            registro_de_mensajes(f"Cliente eliminado {message.value['customer_id']} no tiene mas servicios",ip_cliente)
            cursor.execute(f"Delete from Cliente where id = '{message.value['customer_id']}'")
            conn.commit()
            imprimir()
        else:
            ip_cliente=message.value["ip"]
            taxi_disponible=seleccionar_taxi()
            if taxi_disponible:
                print(f"Taxi disponible ID:  {taxi_disponible[0]} para el cliente {message.value['customer_id']}")
                enviar_mensaje(f"Taxi disponible ID:  {taxi_disponible[0]} para el cliente {message.value['customer_id']}")
                registro_de_mensajes(f"Taxi disponible ID:  {taxi_disponible[0]} para el cliente {message.value['customer_id']}",ip_taxis)
                cursor.execute(f"Select * from Cliente where id = '{message.value['customer_id']}'")
                total=cursor.fetchone()
                if total is  None:
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
                
                imprimir()
                time.sleep(0.5)
                producer_client.send('Servicios_cliente',value={"customer_id":message.value["customer_id"],"taxi_id":taxi_disponible[0]})
                clave_aes = obtener_clave(taxi_disponible[0])
                mensaje_prod = {"id":taxi_disponible[0],"Estado":"RUN","destino":message.value["pickup_location"]}
                mensaje_prod_json = json.dumps(mensaje_prod)
                encrypted_message = encrypt_message(bytes.fromhex(clave_aes), mensaje_prod_json)
                producer_taxi.send('Servicio_taxi',{"id":taxi_disponible[0],"data":encrypted_message.hex()})
            else:
                print("No hay taxis disponibles.")
                time.sleep(0.5)
                producer_client.send('Servicios_cliente',value={"customer_id":message.value["customer_id"],"taxi_id":None})
   


def enviar_mapa():
    
    ubicaciones_clientes, ubicaciones_taxi=leer_bbdd('Taxi.db')
    message={
        'ubicaciones':ubicaciones,
        'ubicaciones_cliente':ubicaciones_clientes,
        'ubicaciones_taxi':ubicaciones_taxi
    }
    producer_enviar_mapa.send('Mapa',message)
    

 
               
def movimientos_taxi():
    global token
    conn = create_connection()
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Movs'...")
    for message in consumer_taxi_movs:
        taxi_id = message.value["id"]
        clave_aes = obtener_clave(taxi_id)
        encrypted_data = message.value["data"] 
        if clave_aes is not None:
            decrypted_message = decrypt_message(bytes.fromhex(clave_aes), encrypted_data)
            mensaje_json = decrypted_message.decode()
            mensaje = json.loads(mensaje_json)
            print(f"Mensaje recibido: {mensaje['Movimiento']},"f"ID: {taxi_id}")
            cursor.execute(f"Select token from Taxi where id = {taxi_id}")
            token_bd=cursor.fetchone()
            if token_bd[0] == mensaje["token"]:
                cursor.execute(f"Select posx,posy from Taxi where id = {taxi_id}")
                posx,posy=cursor.fetchone()
                
                direc=DIRECTIONS[mensaje["Movimiento"]]
                posx+=direc[0]
                posy+=direc[1]
                cursor.execute(f"Select pasajero from Taxi where id = {taxi_id}")
                pasajero=cursor.fetchone()
                
                if  pasajero is not None:
                    cursor.execute(f"Update Cliente set posx = {posx} where id = '{pasajero[0]}'")
                    cursor.execute(f"Update Cliente set posy = {posy} where id = '{pasajero[0]}'")
                cursor.execute(f"Update Taxi set posx = {posx}, posy = {posy} where id = {taxi_id}")
                conn.commit()
                imprimir()
            else:
                print(f"Token no válido para el id {taxi_id}")
                enviar_mensaje(f"Token no válido para el id {taxi_id}")
                registro_de_mensajes(f"Token no válido para el id {taxi_id}")
        
    







def imprimir_base_datos_cliente():
    try:
        conn = create_connection()
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
    except Error as e:
        print(f"Error al intentar leer la base de datos: {e}")
    finally:
        conn.close()








def leer_bbdd(db_path):
    ubicaciones_clientes = {}  # Leo de bbdd y guardo ubis de clientes
    ubicaciones_taxi = {}  # Leo de bbdd y guardo ubis de taxis

    # Conexion a la base de datos
    conn = create_connection()
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
        id, posx, posy, estado, destino1, destino2, pasajero,token,clave = taxi
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
            elif taxi_info['estado'] == 'OKP':
                taxi_str = f"{Fore.RED}{id_taxi}{Style.RESET_ALL}"
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
        
        
if __name__ == "__main__":
    
    ip_central = get_wifi_ip()

    ubicaciones = leer_mapa('mapa.txt')
    ubicaciones_clientes, ubicaciones_taxi = leer_bbdd('Taxi.db')
    imprimir_mapa(crear_mapa(ubicaciones, ubicaciones_clientes, ubicaciones_taxi))
    imprimir_base_datos()
    imprimir_base_datos_cliente()   
    start_server(int(puerto))  

    
