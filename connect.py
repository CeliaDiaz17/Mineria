import cx_Oracle

# Configura la cadena de conexión
connection = cx_Oracle.connect(
    user="USER_W",
    password="3VypJ8EZeVi4HNy",
    dsn="dbmm_high"  # Debe coincidir con el alias en tnsnames.ora
)

# Crea un cursor
cursor = connection.cursor()

# Ejecuta consultas SQL
cursor.execute("SELECT * FROM tu_tabla")
for row in cursor.fetchall():
    print(row)

# Cierra el cursor y la conexión
cursor.close()
connection.close()
