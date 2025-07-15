import pyodbc

# Configuración de conexión
DB_CONFIG = {
    'server': 'DESKTOP-5B78EO8\\SQL2022',
    'database': 'SannaIConformidadRegulatoria',
    'trusted_connection': 'yes',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Datos de usuarios
usuarios = [
    ("PER001", "César Henry", "Vásquez Sánchez", "Ministro de salud"),
    ("PER002", "Juan Antonio", "Almeyda Alcántara", "Director general de donaciones, trasplantes y banco de sangre"),
    ("PER003", "Edwin", "Quispe Quispe", "Director ejecutivo de medicamentos, insumos y drogas"),
    ("PER004", "Segundo", "Montoya Mestanza", "Director general de administración de recursos")
]

# Capitalizar campos correctamente
def capitalizar_nombre(texto):
    return ' '.join([p.capitalize() for p in texto.split()])

def capitalizar_tipo(texto):
    return texto.strip().capitalize()

# Conexión
try:
    conn = pyodbc.connect(
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    print("✅ Conectado a SQL Server")

    for idu, nombre, apellido, tipo in usuarios:
        nombre = capitalizar_nombre(nombre)
        apellido = capitalizar_nombre(apellido)
        tipo = capitalizar_tipo(tipo)

        cursor.execute("""
            INSERT INTO Usuarios (id_usuario, nombre_usuario, apellido_usuario, tipopersona_usuario)
            VALUES (?, ?, ?, ?)
        """, (idu, nombre, apellido, tipo))
        print(f"✅ Usuario {idu} insertado correctamente")

    conn.commit()
    conn.close()
    print("✅ Todos los usuarios fueron insertados.")

except Exception as e:
    print(f"❌ Error al insertar usuarios: {e}")
