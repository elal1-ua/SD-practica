# SD - Práctica Taxis
./bin/zookeeper-server-start.sh ./config/zookeeper.properties <br>
./bin/kafka-server-start.sh ./config/server.properties <br>
docker run -it --network=host [imagen_central] python3 EC_Central.py 2181 [IP_BROKER] 9092 <br>
docker run -it --network=host [imagen_de] python3 EC_DE.py [IP_BROKER] 9092 [IP_CENTRAL] 3001 [id_taxi] <br>
python3 EC_Sk.py [IP_BROKER] 8080 <br> 
docker run -it --network=host [imagen_customer] python3 EC_Customer.py [IP_BROKER] 9092 [id_customer] x y services.txt <br>
uvicorn EC_CTC:app --reload
