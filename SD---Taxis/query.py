import sqlite3

# Conectar a la base de datos
conn = sqlite3.connect('Taxi.db')
cursor = conn.cursor()

# Ejecutar una sentencia SQL
cursor.execute("Update Taxi SET posx=15, posy=15 WHERE id=1") 
cursor.execute("SELECT * FROM Taxi")

# Obtener los resultados
rows = cursor.fetchall()
for row in rows:
    print(row)

# Cerrar la conexión
conn.close()