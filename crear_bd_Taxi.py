import sqlite3

def crear_base_de_datos():
    
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        del_sql = "DROP TABLE IF EXISTS Taxi"
        cursor.execute(del_sql)
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
        
        cursor.execute('''INSERT INTO Taxi (id, posx, posy, estado) VALUES (1, 1, 1, "KO")''')
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error al crear la base de datos: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    #crear_base_de_datos()
    conn = sqlite3.connect('Taxi.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Cliente where id = 'a'") 
    conn.commit()