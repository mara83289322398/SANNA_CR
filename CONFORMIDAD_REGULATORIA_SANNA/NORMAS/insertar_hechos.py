import pyodbc

# Configuración
DB_CONFIG = {
    'server': 'DESKTOP-5B78EO8\\SQL2022',
    'database': 'SannaIConformidadRegulatoria',
    'trusted_connection': 'yes',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

try:
    # Conexión a SQL Server
    conn = pyodbc.connect(
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    print("✅ Conexión exitosa a SQL Server")

    # Obtener datos de la tabla Normativas
    cursor.execute("""
        SELECT id_normativa, id_usuario, sucursal_id, fecha, resultado_normativa
        FROM Normativas
    """)
    filas = cursor.fetchall()

    for fila in filas:
        id_normativa, id_usuario, sucursal_id, fecha, resultado = fila

        total = 1
        conformes = 1 if resultado.lower() == "conforme" else 0
        no_conformes = 1 if resultado.lower() == "no conforme" else 0

        cursor.execute("""
            INSERT INTO Hechos_Conformidad_Sanitaria (
                sucursal_id, id_usuario, id_normativa, fecha,
                total_acciones_correctivas,
                total_acciones_correctivas_conformes,
                total_acciones_correctivas_noconformes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (sucursal_id, id_usuario, id_normativa, fecha, total, conformes, no_conformes))

        print(f"✅ Insertado: {id_normativa} - {resultado}")

    conn.commit()
    conn.close()
    print("🎉 Todos los registros insertados correctamente en Hechos_Conformidad_Sanitaria")

except Exception as e:
    print(f"❌ Error: {e}")
