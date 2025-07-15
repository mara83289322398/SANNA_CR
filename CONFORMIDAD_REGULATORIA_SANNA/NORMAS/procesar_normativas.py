import os
import re
import fitz  # PyMuPDF
import pyodbc
import requests
from bs4 import BeautifulSoup
import unicodedata
from datetime import datetime
import calendar

# CONFIGURACIÓN DE CONEXIÓN
DB_CONFIG = {
    'server': 'DESKTOP-5B78EO8\\SQL2022',
    'database': 'SannaIConformidadRegulatoria',
    'trusted_connection': 'yes',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        connection_string = (
            f"DRIVER={self.config['driver']};"
            f"SERVER={self.config['server']};"
            f"DATABASE={self.config['database']};"
            f"Trusted_Connection=yes;"
        )
        self.connection = pyodbc.connect(connection_string)
        print("Conexion exitosa a SQL Server")

    def get_connection(self):
        if self.connection is None:
            self.connect()
        return self.connection

# NORMALIZACIÓN

def normalizar(texto):
    texto = texto.lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    texto = re.sub(r"[\\’'\"–\-/()\[\]]", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()

def sucursal_mas_cercana(extraida):
    extraida_norm = normalizar(extraida)
    palabras_extraida = set(extraida_norm.split())
    mejor_match = None
    mejor_puntaje = 0
    for original, normalizado in zip(sucursales_raw, sucursales_db):
        palabras_sucursal = set(normalizado.split())
        comunes = palabras_extraida.intersection(palabras_sucursal)
        puntaje = len(comunes)
        if puntaje > mejor_puntaje:
            mejor_puntaje = puntaje
            mejor_match = original
    if mejor_puntaje >= 2 or extraida_norm in sucursales_db:
        return mejor_match
    return None

def extraer_tipo(soup):
    h2 = soup.find("h2")
    return h2.get_text(strip=True) if h2 else "SIN TIPO"

def extraer_fecha(soup):
    h2 = soup.find("h2")
    if not h2:
        return None
    p_tag = h2.find_next_sibling("p")
    if p_tag:
        texto = p_tag.get_text(strip=True)
        meses = {
            "enero": "January", "febrero": "February", "marzo": "March",
            "abril": "April", "mayo": "May", "junio": "June",
            "julio": "July", "agosto": "August", "setiembre": "September",
            "septiembre": "September", "octubre": "October",
            "noviembre": "November", "diciembre": "December"
        }
        for es, en in meses.items():
            texto = texto.lower().replace(es, en)
        try:
            fecha = datetime.strptime(texto, "%d de %B de %Y")
            return fecha
        except Exception:
            pass
    return None

def extraer_accion(html_text):
    match = re.search(r"(Ot[óo]rguese|Decl[áa]rese|Autoriz[ae]|Enc[áa]rg[ue])[^.]{20,200}\.", html_text, re.IGNORECASE)
    return match.group(0).strip() if match else "SIN ACCIONES"

def extraer_sucursal(texto):
    patrones = [
        r'nombre comercial\s*[“"]?(SANNA[^”",\n]+)',
        r'autorizaci[oó]n.*?a\s+(SANNA[^”",\n]+)',
        r'(SANNA[^,.\n]{5,80})'
    ]
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

def insertar_tiempo(cursor, fecha):
    try:
        cursor.execute("SELECT id_tiempo FROM Tiempo WHERE fecha = ?", fecha)
        row = cursor.fetchone()
        if row:
            return row[0]
        año = fecha.year
        mes = fecha.month
        dia_año = fecha.timetuple().tm_yday
        semana_año = int(fecha.strftime("%U"))
        trimestre = (mes - 1) // 3 + 1
        dia_semana = calendar.day_name[fecha.weekday()]
        año_mes = f"{año}-{mes:02d}"
        cursor.execute("""
            INSERT INTO Tiempo (año_mes, dia_semana, trimestre, dia_año, semana_año, mes, año, fecha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (año_mes, dia_semana, trimestre, dia_año, semana_año, mes, año, fecha))
        cursor.execute("SELECT @@IDENTITY")
        return int(cursor.fetchone()[0])
    except Exception as ex:
        print(f"Error insertando tiempo: {ex}")
        return None

# RELACIÓN ID_USUARIO POR NOR
mapa_usuarios = {
    "NOR001": "PER001",
    "NOR002": "PER002",
    "NOR003": "PER002",
    "NOR004": "PER002",
    "NOR005": "PER002",
    "NOR006": "PER003",
    "NOR007": "PER003",
    "NOR009": "PER003",
    "NOR008": "PER004"
}

# CONEXIÓN

db = DatabaseManager(DB_CONFIG)
conn = db.get_connection()
cursor = conn.cursor()

cursor.execute("SELECT nombre, id FROM Sucursales")
sucursales_raw = []
sucursales_db = []
sucursal_ids = {}
for row in cursor.fetchall():
    nombre_original = row.nombre
    id_sucursal = row.id
    sucursales_raw.append(nombre_original)
    sucursales_db.append(normalizar(nombre_original))
    sucursal_ids[normalizar(nombre_original)] = id_sucursal

# URLs
with open("urlnormas.txt", "r", encoding="utf-8") as f:
    urls = [line.strip() for line in f.readlines()]

for i, url in enumerate(urls):
    nombre_pdf = f"NOR{str(i+1).zfill(3)}.pdf"
    ruta_pdf = os.path.join("normativas", nombre_pdf)
    print(f"\nProcesando {nombre_pdf} desde {url}")
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        html_text = soup.get_text()

        tipo = extraer_tipo(soup)
        print(f"Tipo extraído: {tipo}")

        fecha = extraer_fecha(soup)
        if fecha:
            print(f"Fecha: {fecha.strftime('%d/%m/%Y')}")
            tiempo_id = insertar_tiempo(cursor, fecha)
        else:
            print("⚠️ Fecha no encontrada.")
            tiempo_id = None

        sucursal_extraida = extraer_sucursal(html_text)
        if sucursal_extraida:
            print(f"Sucursal extraída: {sucursal_extraida}")
            match = sucursal_mas_cercana(sucursal_extraida)
            if match:
                print(f"Coincidencia encontrada en lista: {match}")
                sucursal_id = sucursal_ids.get(normalizar(match))
            else:
                print(f"No se encontró coincidencia válida para: {sucursal_extraida}")
                sucursal_id = None
        else:
            print("⚠️ No se pudo extraer una sucursal del HTML")
            sucursal_id = None

        acciones = extraer_accion(html_text)
        print(f"Acciones correctivas: {acciones}")

        estado = "Activo"
        resultado = "Conforme" if re.search(r"ot[oó]rg[ao]|autoriz[ao]", acciones, re.IGNORECASE) else "No conforme"

        id_normativa = nombre_pdf[:-4]
        id_usuario = mapa_usuarios.get(id_normativa)

        if tipo != "SIN TIPO" and sucursal_id and tiempo_id and id_usuario:
            cursor.execute("""
                INSERT INTO Normativas (
                id_normativa, tipo_normativa, estado_normativa,
                resultado_normativa, acciones_normativa,
                sucursal_id, id_usuario, fecha, id_tiempo
            )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (id_normativa, tipo, estado, resultado, acciones, sucursal_id, id_usuario, fecha, tiempo_id))




            conn.commit()
            print(f"✅ Normativa {id_normativa} insertada correctamente con usuario {id_usuario}")
        else:
            print("⚠️ Normativa no insertada por datos incompletos")

    except Exception as e:
        print(f"❌ Error procesando {nombre_pdf}: {e}")

conn.close()
