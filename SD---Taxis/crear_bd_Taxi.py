import sqlite3

def crear_base_de_datos():
    
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        del_sql = "DROP TABLE IF EXISTS Taxi"
        del_sql1="DROP TABLE IF EXISTS Cliente"
        cursor.execute(del_sql)
        cursor.execute(del_sql1)
        conn.commit()
        # Crear tabla ID
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Taxi (
            id INTEGER PRIMARY KEY,
            posx INTEGER NOT NULL,
            posy INTEGER NOT NULL,
            estado CHAR(50) NOT NULL,
            destino1 CHAR(50),
            destino2 CHAR(50),
            pasajero CHAR(50)           
        );
        ''')
        conn.commit()
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (1, 1, 1, "ND")''')
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (2, 1, 1, "ND")''')
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (3, 1, 1, "ND")''')
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (4, 1, 1, "ND")''')
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (5, 1, 1, "ND")''')
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (6, 1, 1, "ND")''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Cliente (
            id CHAR(50) PRIMARY KEY,
            posx INTEGER NOT NULL,
            posy INTEGER NOT NULL,
            destino CHAR(50),
            estado CHAR(50) NOT NULL          
        );
        ''')
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error al crear la base de datos: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    
    crear_base_de_datos()
    print("Base de datos creada con éxito.")
