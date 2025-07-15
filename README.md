-----------------------------------------------------------------------
# Proyecto: SannaIConformidadRegulatoria (ETL)

Este proyecto implementa un proceso ETL para cargar datos sobre normativas sanitarias extraídas desde gob.pe y documentos PDF hacia una base de datos SQL Server.

-----------------------------------------------------------------------

##  Archivos del proyecto

- `Big-Data-Maps.py`: Inserta datos en la tabla Sucursales.
- `urls.txt`: Contiene las url analizadas para extraes datos para la tabla Sucursales.
- `usuarios.py`: Inserta datos de los firmantes (tabla Usuarios).
- `procesar_normativas.py`: Extrae, transforma y carga las normativas, fechas y relaciones.
- `urlnormas.txt`: URLs de cada normativa desde gob.pe (una por línea).
- `normativas/`: Carpeta con los archivos PDF de las normativas (`NOR001.pdf`, `NOR002.pdf`, ...).
- `insertar_hechos.py`: Inserta y hace un conteo de las acciones conformes/no conformes.
  
-----------------------------------------------------------------------

### Requisitos
- Visual Code
- Python 3.10+
- SQL Server con base de datos creada: SannaIConformidadRegulatoria
- Instalar librerías necesarias:

```bash
pip install requests beautifulsoup4 PyMuPDF pyodbc

-----------------------------------------------------------------------

##   Paso 1: Configurar conexión a SQL Server
Editar la sección DB_CONFIG en ambos scripts .py:

DB_CONFIG = {
    'server': 'TU_SERVIDOR\\INSTANCIA',
    'database': 'SannaIConformidadRegulatoria',
    'trusted_connection': 'yes',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

-----------------------------------------------------------------------

##  Paso 2: Ejecutar el proceso ETL

1. Insertar sucursales (desde Google Maps)
Ubicado en la carpeta MAPS:

cd MAPS
python Big-Data-Maps.py

2. Insertar normativas (web + PDF)
Asegúrate de tener la carpeta normativas/ con PDFs y el archivo urlnormas.txt. Luego, desde la carpeta NORMAS:

cd NORMAS
python procesar_normativas.py

3. Insertar usuarios (firmantes de normativas)
Desde la misma carpeta NORMAS:
python usuarios.py

4. Insertar hechos de conformidad sanitaria
Este script genera los totales de acciones correctivas por normativa y los inserta en la tabla de hechos. Ejecutar:

python insertar_hechos.py

-----------------------------------------------------------------------
