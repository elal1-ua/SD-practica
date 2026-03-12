import sys
import time
import json
import threading
from kafka import KafkaProducer
from kafka import KafkaConsumer
class ECCustomer:
    def __init__(self, broker_ip, broker_port, customer_id, pickup_x, pickup_y, file_path):
        # Conexión al broker de Kafka
        self.producer = KafkaProducer( 
            bootstrap_servers=broker_ip + ':' + broker_port,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.consumer_posicion = KafkaConsumer( 
            'Posicion_cliente',
            bootstrap_servers=broker_ip + ':' + broker_port,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.consumer = KafkaConsumer( 
            'Servicios_cliente',
            bootstrap_servers=broker_ip + ':' + broker_port,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            
        )
        self.customer_id = customer_id
        self.pickup_location = (pickup_x, pickup_y)  # Almacena las coordenadas X e Y
        self.file_path = file_path
        self.contador_activo = True
        self.error = False
        self.last_message_time =8 
    
    def read_services(self):
        # Leer las solicitudes de servicio del archivo
        with open(self.file_path, 'r') as file:
            services = file.readlines()
        # Eliminar saltos de línea y devolver la lista de destinos
        return [service.strip() for service in services] #strip() elimina los saltos de linea

    def send_service_request(self, pickup, destination):
        # Formato del mensaje que se enviará a la central
        message = {
            'customer_id': self.customer_id,
            'pickup_location': pickup,
            'destination': destination,
            
        }
        print(f"Solicitando servicio de taxi desde {pickup} hasta {destination}")
        # Enviar el mensaje al topic de Kafka
        self.producer.send('Servicios', value=message)
        
        threading.Thread(target=self.contador, args=(), daemon=True).start() #Hilo que se encarga de contar el tiempo que ha pasado desde que se envio el mensaje
        self.contador_activo = True
        
        for msg in self.consumer: #HACER UN HILO PARA QUE ESTE LEYENDO SI LA CENTRAL HA FALLADO (customer_id == "all")
            print("Mensaje recibido")
            if msg.value['customer_id'] != "all":#La central envia el mensaje a todos los clientes de que ha tenido un error
                if msg.value['customer_id'] == self.customer_id:
                    self.contador_activo = False
                    self.last_message_time =8 
                    print(f"Taxi {msg.value['taxi_id']} asignado para el servicio de {pickup} a {destination}")
                    if msg.value['taxi_id'] != None:
                        for message in self.consumer_posicion:
                            if message.value['customer_id'] == self.customer_id:
                                print(f"Taxi {message.value}") 
                                self.pickup_location = message.value['cliente_pos'] 
                                break
                            elif message.value['customer_id'] == "all":
                                print("Error: La central ha fallado")
                                exit(1)
                            
                    else:
                        print("Error: No hay taxis disponibles")
                            
                    break     
            else:
                print("Error: La conexion con la central ha fallado")                  
                exit(1)

    
    def contador(self):
        while self.contador_activo:
            self.last_message_time -= 1
            if self.last_message_time == 0: 
                print("Error: El cliente ya existe en la bd")
                self.error = True
                exit(1)
            time.sleep(1)  
    
    def run(self): 
        # Leer los destinos de servicio
        services = self.read_services() 
        
        for service in services:
            # Enviar la solicitud de taxi
            self.send_service_request(self.pickup_location, service)
            # Esperar 4 segundos antes de enviar la siguiente solicitud
            
            time.sleep(4)
        print("No hay más destinos.Enviando señal a la central")
        self.producer.send('Servicios', value={"customer_id": self.customer_id, "pickup_location": self.pickup_location, "destination": None})
        time.sleep(4)

if __name__ == '__main__': 
    # Verificar que se recibieron los parámetros correctos
    if len(sys.argv) != 7:
        print("Uso: python EC_Customer.py <broker_ip> <broker_port> <customer_id> <pickup_x> <pickup_y> <file_path>")
        sys.exit(1)

    # Leer los argumentos de la línea de comandos
    broker_ip = sys.argv[1]
    broker_port = sys.argv[2]
    customer_id = sys.argv[3]
    pickup_x = int(sys.argv[4])  # Convertir a entero
    pickup_y = int(sys.argv[5])  # Convertir a entero
    file_path = sys.argv[6]

    # Crear la aplicación cliente y ejecutar
    customer = ECCustomer(broker_ip, broker_port, customer_id, pickup_x, pickup_y, file_path)
    customer.run()