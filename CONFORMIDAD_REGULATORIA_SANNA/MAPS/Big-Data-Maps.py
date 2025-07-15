from playwright.sync_api import sync_playwright
import re
import json
import time
import os
import pyodbc
from datetime import datetime
from collections import Counter

try:
    import nltk
    from textblob import TextBlob
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    import spacy
    
    try:
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
    except:
        pass
        
except ImportError as e:
    print(f"⚠️ Error importando librerías de análisis: {str(e)}")
    print("💡 Instala las dependencias: pip install textblob vaderSentiment spacy nltk")

DB_CONFIG = {
    'server': 'DESKTOP-5B78EO8\SQL2022',
    'database': 'SannaIConformidadSanitaria',
    'trusted_connection': 'yes',
    'driver': '{ODBC Driver 17 for SQL Server}'
}
class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Conectar a la base de datos"""
        try:
            if 'trusted_connection' in self.config:
                connection_string = (
                    f"DRIVER={self.config['driver']};"
                    f"SERVER={self.config['server']};"
                    f"DATABASE={self.config['database']};"
                    f"Trusted_Connection=yes"
                )
            else:
                connection_string = (
                    f"DRIVER={self.config['driver']};"
                    f"SERVER={self.config['server']};"
                    f"DATABASE={self.config['database']};"
                    f"UID={self.config['username']};"
                    f"PWD={self.config['password']}"
                )
            
            self.connection = pyodbc.connect(connection_string)
            print("✅ Conexión a SQL Server establecida")
            return True
        except Exception as e:
            print(f"❌ Error conectando a SQL Server: {str(e)}")
            print("💡 Verifica la configuración en DB_CONFIG")
            return False
    
    def disconnect(self):
        """Desconectar de la base de datos"""
        if self.connection:
            self.connection.close()
            print("🔌 Conexión a SQL Server cerrada")
    
    def insert_sucursal(self, data):
        """Insertar datos de sucursal y retornar el ID"""
        try:
            cursor = self.connection.cursor()
            
            # Primero verificar si ya existe
            check_query = "SELECT id FROM Sucursales WHERE url = ?"
            cursor.execute(check_query, (data['url'],))
            existing = cursor.fetchone()
            
            if existing:
                print(f"⚠️ La URL ya existe, usando sucursal existente con ID: {existing[0]}")
                return existing[0]
            
            # Insertar nueva sucursal
            query = """
            INSERT INTO Sucursales (url, nombre, ubicacion, sitio_web, telefono, referencia)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(query, (
                data['url'],
                data['nombre'],
                data['ubicacion'],
                data['info_adicional']['sitio_web'],
                data['info_adicional']['telefono'],
                data['info_adicional']['referencia']
            ))
            
            sucursal_id = cursor.fetchone()[0]
            self.connection.commit()
            
            print(f"✅ Sucursal insertada con ID: {sucursal_id}")
            return sucursal_id
            
        except Exception as e:
            print(f"❌ Error insertando sucursal: {str(e)}")
            return None

    def get_sucursal_id_by_url(self, url):
        """Obtener ID de sucursal por URL"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM Sucursales WHERE url = ?", (url,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"❌ Error obteniendo ID de sucursal: {str(e)}")
            return None
    
    def insert_calificacion(self, sucursal_id, data):
        """Insertar calificación global"""
        try:
            cursor = self.connection.cursor()
            
            rating_global = float(data['rating_global']) if data['rating_global'] else None
            total_reviews = int(data['total_reviews']) if data['total_reviews'].isdigit() else 0
            
            query = """
            INSERT INTO Calificaciones (sucursal_id, rating_global, total_reviews)
            VALUES (?, ?, ?)
            """
            
            cursor.execute(query, (sucursal_id, rating_global, total_reviews))
            self.connection.commit()
            
            print(f"✅ Calificación insertada para sucursal {sucursal_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error insertando calificación: {str(e)}")
            return False
    
    def insert_horarios(self, sucursal_id, horarios):
        """Insertar horarios de atención"""
        try:
            cursor = self.connection.cursor()
            
            for horario in horarios:
                esta_cerrado = 1 if 'cerrado' in horario['horas'].lower() else 0
                
                query = """
                INSERT INTO Horarios (sucursal_id, dia_semana, horas, esta_cerrado)
                VALUES (?, ?, ?, ?)
                """
                
                cursor.execute(query, (
                    sucursal_id,
                    horario['dia'],
                    horario['horas'],
                    esta_cerrado
                ))
            
            self.connection.commit()
            print(f"✅ {len(horarios)} horarios insertados para sucursal {sucursal_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error insertando horarios: {str(e)}")
            return False
    
    def insert_reviews(self, sucursal_id, reviews):
        """Insertar reseñas"""
        try:
            cursor = self.connection.cursor()
            inserted_count = 0
            
            for review in reviews:
                try:
                    rating = int(review['rating']) if review['rating'].isdigit() else None
                    likes = int(review['likes']) if review['likes'].isdigit() else 0
                    
                    query = """
                    INSERT INTO Reviews (sucursal_id, autor, rating, fecha_review, texto, cantidad_fotos, likes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    cursor.execute(query, (
                        sucursal_id,
                        review['author'],
                        rating,
                        review['date'],
                        review['text'],
                        review['photos'],
                        likes
                    ))
                    inserted_count += 1
                    
                except Exception as e:
                    print(f"⚠️ Error insertando review individual: {str(e)}")
                    continue
            
            self.connection.commit()
            print(f"✅ {inserted_count} reviews insertados para sucursal {sucursal_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error insertando reviews: {str(e)}")
            return False
    
    def save_complete_data(self, data):
        """Guardar todos los datos de una sucursal"""
        try:
            # 1. Insertar sucursal
            sucursal_id = self.insert_sucursal(data)
            if not sucursal_id:
                return False
            
            # 2. Insertar calificación
            self.insert_calificacion(sucursal_id, data)
            
            # 3. Insertar horarios
            if data['info_adicional']['horarios']:
                self.insert_horarios(sucursal_id, data['info_adicional']['horarios'])
            
            # 4. Insertar reviews
            if data['reviews']:
                self.insert_reviews(sucursal_id, data['reviews'])
            
            print(f"🎉 Datos completos guardados en BD para sucursal {sucursal_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando datos completos: {str(e)}")
            return False

def scrape_google_maps(url):
    """Función de scraping de Google Maps con scroll completo para todas las reseñas"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            viewport={"width": 1200, "height": 800}
        )
        page = context.new_page()
        
        try:
            # Navegar a la URL
            page.goto(url, timeout=60000)
            page.wait_for_selector('h1', timeout=30000)
            
            # Aceptar cookies
            accept_button = page.query_selector('button:has-text("Aceptar todo"), button:has-text("Accept all")')
            if accept_button:
                accept_button.click()
                page.wait_for_timeout(1000)
            
            # Extraer nombre completo con selector mejorado
            nombre = ""
            main_title = page.query_selector('h1.DUwDvf.lfPIob')
            if main_title:
                nombre = main_title.inner_text().strip()
            
            # Extraer subtítulo si existe
            subtitle = page.query_selector('h2.bwoZTb.fontBodyMedium span')
            if subtitle:
                nombre += " - " + subtitle.inner_text().strip()
            
            # Extraer dirección con selector mejorado
            address_btn = page.query_selector('button[data-item-id="address"], button[data-tooltip="Copiar dirección"]')
            if address_btn:
                address_element = address_btn.query_selector('.Io6YTe')
                if address_element:
                    ubicacion = address_element.inner_text()
                else:
                    ubicacion = address_btn.inner_text()
            else:
                ubicacion = "No encontrada"
            
            # SOLUCIÓN CORREGIDA PARA RATING GLOBAL Y TOTAL REVIEWS
            rating_global = None
            total_reviews = "0"
            
            # Estrategia 1: Buscar rating en el bloque principal
            rating_block = page.query_selector('div.F7nice')
            if rating_block:
                # Extraer rating global
                rating_span = rating_block.query_selector('span[aria-hidden="true"]')
                if rating_span:
                    rating_text = rating_span.inner_text().strip()
                    if re.match(r'^\d+[.,]\d+$', rating_text):
                        rating_global = rating_text.replace(',', '.')
                
                # Extraer total de reviews
                reviews_span = rating_block.query_selector('span[aria-label]')
                if reviews_span:
                    reviews_text = reviews_span.get_attribute('aria-label')
                    if reviews_text:
                        total_reviews_match = re.search(r'(\d+)', reviews_text)
                        if total_reviews_match:
                            total_reviews = total_reviews_match.group(1)
            
            # Estrategia 2: Si no se encontró rating, buscar en otro lugar
            if not rating_global:
                rating_element = page.query_selector('div[role="img"][aria-label*="star"], div[role="img"][aria-label*="estrella"]')
                if rating_element:
                    rating_label = rating_element.get_attribute('aria-label')
                    if rating_label:
                        rating_match = re.search(r'(\d+[.,]\d+)', rating_label)
                        if rating_match:
                            rating_global = rating_match.group(1).replace(',', '.')
            
            # Estrategia 3: XPath específico para total reviews si no se encontró
            if total_reviews == "0":
                try:
                    xpath = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[2]/span/span'
                    reviews_element = page.query_selector(f'xpath={xpath}')
                    if reviews_element:
                        reviews_text = reviews_element.inner_text()
                        total_reviews_match = re.search(r'(\d+)', reviews_text)
                        if total_reviews_match:
                            total_reviews = total_reviews_match.group(1)
                except Exception as e:
                    print(f"Error con XPath específico: {str(e)}")
            
            # Extraer información adicional
            info_adicional = {
                'horarios': [],
                'sitio_web': None,
                'telefono': None,
                'referencia': None
            }
            
            # Horarios de atención
            horarios_table = page.query_selector('table.eK4R0e')
            if horarios_table:
                dias = horarios_table.query_selector_all('tr.y0skZc')
                for dia in dias:
                    try:
                        dia_element = dia.query_selector('td.ylH6lf')
                        horas_element = dia.query_selector('td.mxowUb')
                        if dia_element and horas_element:
                            nombre_dia = dia_element.inner_text().strip()
                            horas = horas_element.inner_text().strip()
                            info_adicional['horarios'].append({
                                'dia': nombre_dia,
                                'horas': horas
                            })
                    except:
                        continue
            
            # Sitio web
            sitio_web_element = page.query_selector('a[data-item-id="authority"]')
            if sitio_web_element:
                sitio_web_text = sitio_web_element.query_selector('.Io6YTe')
                if sitio_web_text:
                    info_adicional['sitio_web'] = sitio_web_text.inner_text().strip()
            
            # Teléfono
            telefono_element = page.query_selector('button[data-item-id^="phone"]')
            if telefono_element:
                telefono_text = telefono_element.query_selector('.Io6YTe')
                if telefono_text:
                    info_adicional['telefono'] = re.sub(r'[^\d\+\-\s\(\)]', '', telefono_text.inner_text())
            
            # Referencia (Plus code)
            referencia_element = page.query_selector('button[data-item-id="oloc"]')
            if referencia_element:
                referencia_text = referencia_element.query_selector('.Io6YTe')
                if referencia_text:
                    info_adicional['referencia'] = referencia_text.inner_text().strip()
            
            # ========================================================================
            # SECCIÓN DE EXTRACCIÓN DE RESEÑAS CON SCROLL COMPLETO
            # ========================================================================
            
            # Navegar a la sección de opiniones
            opiniones_tab = page.query_selector('button:has-text("Opiniones"), button:has-text("Reviews")')
            if opiniones_tab and opiniones_tab.is_visible():
                try:
                    opiniones_tab.click()
                    page.wait_for_selector('.jftiEf', timeout=10000)
                    page.wait_for_timeout(3000)
                except:
                    print("No se pudo hacer clic en la pestaña de opiniones")
            
            # Identificar contenedor de opiniones
            opiniones_container = page.query_selector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
            if not opiniones_container:
                opiniones_container = page.query_selector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd')
            
            # Función para hacer scroll y cargar más opiniones
            def scroll_and_load_reviews():
                if opiniones_container:
                    # Hacer scroll hasta el fondo del contenedor
                    opiniones_container.evaluate('element => element.scrollTop = element.scrollHeight')
                    page.wait_for_timeout(2000)
                    
                    # Verificar si hay más reviews cargados
                    new_reviews = page.query_selector_all('.jftiEf')
                    return len(new_reviews)
                return 0
            
            print("🔄 Cargando todas las reseñas...")
            
            # Cargar todos los reviews mediante scrolling
            last_count = 0
            current_count = len(page.query_selector_all('.jftiEf'))
            attempts = 0
            max_attempts = 20  # Máximo número de intentos sin nuevos reviews
            
            while attempts < max_attempts:
                last_count = current_count
                current_count = scroll_and_load_reviews()
                
                print(f"📝 Reviews cargados: {current_count}")
                
                # Verificar si hemos cargado nuevos reviews
                if current_count > last_count:
                    attempts = 0  # Resetear contador si encontramos nuevos reviews
                    print(f"✅ Se cargaron {current_count - last_count} nuevos reviews")
                else:
                    attempts += 1
                    print(f"⏳ Intento {attempts}/{max_attempts} sin nuevos reviews")
                
                # Intentar hacer clic en "Más reseñas" si está visible
                more_reviews_btn = page.query_selector('button:has-text("Más reseñas"), button:has-text("More reviews")')
                if more_reviews_btn and more_reviews_btn.is_visible():
                    try:
                        # Obtener cantidad de reseñas adicionales
                        count_span = more_reviews_btn.query_selector('div > span')
                        if count_span:
                            count_text = count_span.inner_text()
                            count_match = re.search(r'(\d+)', count_text)
                            count = count_match.group(1) if count_match else "?"
                            print(f"🔄 Cargando {count} reseñas adicionales...")
                        
                        more_reviews_btn.click()
                        page.wait_for_timeout(3000)
                        current_count = len(page.query_selector_all('.jftiEf'))
                        attempts = 0  # Resetear contador después de hacer clic
                    except Exception as e:
                        print(f"⚠️ No se pudo hacer clic en 'Más reseñas': {str(e)}")
                        attempts += 1
                
                # Hacer scroll adicional en la página principal por si acaso
                try:
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    page.wait_for_timeout(1000)
                except:
                    pass
                
                # Salir si no hay cambios después de varios intentos
                if attempts >= 5 and current_count == last_count:
                    print(f"🛑 No se encontraron más reviews después de {attempts} intentos")
                    break
                
                # Límite de seguridad para evitar bucles infinitos
                if current_count > 1000:  # Ajusta este límite según tus necesidades
                    print(f"⚠️ Límite de seguridad alcanzado: {current_count} reviews")
                    break
            
            # Extraer todos los reviews visibles
            review_elements = page.query_selector_all('.jftiEf')
            print(f"🎉 Total de reseñas encontradas: {len(review_elements)}")
            
            reviews = []
            for i, review in enumerate(review_elements):
                try:
                    # Mostrar progreso cada 10 reviews
                    if (i + 1) % 10 == 0:
                        print(f"📝 Procesando review {i + 1}/{len(review_elements)}")
                    
                    author_element = review.query_selector('.d4r55')
                    author = author_element.inner_text() if author_element else "Anónimo"
                    
                    stars = review.query_selector_all('.hCCjke.NhBTye')
                    rating = str(len(stars)) if stars else "0"
                    
                    date_element = review.query_selector('.rsqaWe')
                    date = date_element.inner_text() if date_element else None
                    
                    text_element = review.query_selector('.wiI7pd')
                    text = text_element.inner_text() if text_element else ""
                    
                    photo_count = 0
                    photo_element = review.query_selector('.RfnDt:has-text("photo"), .RfnDt:has-text("foto")')
                    if photo_element:
                        photos_text = photo_element.inner_text()
                        photos_match = re.search(r'(\d+)', photos_text)
                        if photos_match:
                            photo_count = int(photos_match.group(1))
                    
                    likes_element = review.query_selector('button[aria-label*="útil"] .znYl0 > span')
                    likes = likes_element.inner_text() if likes_element else "0"
                    
                    reviews.append({
                        'author': author,
                        'rating': rating,
                        'date': date,
                        'text': text,
                        'photos': photo_count,
                        'likes': likes
                    })
                    
                except Exception as e:
                    print(f"⚠️ Error procesando review {i + 1}: {str(e)}")
                    continue
            
            print(f"✅ Se procesaron exitosamente {len(reviews)} reseñas")
            
            return {
                'url': url,
                'nombre': nombre,
                'ubicacion': ubicacion,
                'rating_global': rating_global,
                'total_reviews': total_reviews,
                'info_adicional': info_adicional,
                'reviews': reviews
            }
        
        except Exception as e:
            print(f"❌ Error general en scraping: {str(e)}")
            return None
        finally:
            browser.close()

def read_urls_from_file(filename):
    """Lee las URLs desde un archivo de texto"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return urls
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {filename}")
        return []
    except Exception as e:
        print(f"Error leyendo el archivo {filename}: {str(e)}")
        return []

def save_result_to_json(resultado, index):
    """Guarda el resultado en un archivo JSON numerado (opcional)"""
    filename = f'info-{index}.json'
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, indent=2, ensure_ascii=False)
        print(f"✅ Backup JSON guardado en {filename}")
        return True
    except Exception as e:
        print(f"❌ Error guardando {filename}: {str(e)}")
        return False

class SentimentAnalyzer:
    def __init__(self, db_manager):
        self.db = db_manager
        
        try:
            self.vader_analyzer = SentimentIntensityAnalyzer()
        except:
            print("⚠️ VADER no disponible")
            self.vader_analyzer = None
        
        # Cargar modelo de spaCy para español
        try:
            self.nlp = spacy.load("es_core_news_sm")
        except OSError:
            print("⚠️ Modelo de spaCy no encontrado. Instalando...")
            os.system("python -m spacy download es_core_news_sm")
            try:
                self.nlp = spacy.load("es_core_news_sm")
            except:
                print("❌ No se pudo cargar spaCy")
                self.nlp = None
        
        # Palabras de parada en español
        self.stop_words = {
            'el', 'la', 'de', 'que', 'y', 'a', 'en', 'un', 'es', 'se', 'no', 'te', 'lo', 'le', 'da', 'su', 'por', 'son', 'con', 'para', 'al', 'del', 'los', 'las', 'una', 'como', 'pero', 'sus', 'me', 'hasta', 'hay', 'donde', 'han', 'quien', 'están', 'estado', 'desde', 'todo', 'nos', 'durante', 'todos', 'uno', 'les', 'ni', 'contra', 'otros', 'ese', 'eso', 'ante', 'ellos', 'e', 'esto', 'mí', 'antes', 'algunos', 'qué', 'unos', 'yo', 'otro', 'otras', 'otra', 'él', 'tanto', 'esa', 'estos', 'mucho', 'quienes', 'nada', 'muchos', 'cual', 'poco', 'ella', 'estar', 'estas', 'algunas', 'algo', 'nosotros', 'mi', 'mis', 'tú', 'te', 'ti', 'tu', 'tus', 'ellas', 'nosotras', 'vosotros', 'vosotras', 'os', 'mío', 'mía', 'míos', 'mías', 'tuyo', 'tuya', 'tuyos', 'tuyas', 'suyo', 'suya', 'suyos', 'suyas', 'nuestro', 'nuestra', 'nuestros', 'nuestras', 'vuestro', 'vuestra', 'vuestros', 'vuestras', 'esos', 'esas'
        }
        
        # Cargar palabras clave desde la base de datos
        self.palabras_clave = self.load_palabras_clave()
    
    def load_palabras_clave(self):
        """Cargar palabras clave desde la base de datos"""
        try:
            cursor = self.db.connection.cursor()
            query = """
            SELECT p.palabra, p.peso, p.tipo, c.nombre as categoria
            FROM PalabrasClave p
            INNER JOIN CategoriasEmocionales c ON p.categoria_emocional_id = c.id
            """
            cursor.execute(query)
            
            palabras = {}
            for row in cursor.fetchall():
                palabras[row.palabra.lower()] = {
                    'peso': float(row.peso),
                    'tipo': row.tipo,
                    'categoria': row.categoria
                }
            
            print(f"✅ Cargadas {len(palabras)} palabras clave para análisis")
            return palabras
            
        except Exception as e:
            print(f"❌ Error cargando palabras clave: {str(e)}")
            return {}
    
    def clean_text(self, text):
        """Limpiar y normalizar texto"""
        if not text:
            return ""
        
        # Convertir a minúsculas
        text = text.lower()
        
        # Remover caracteres especiales pero mantener espacios y puntuación básica
        text = re.sub(r'[^\w\s\.\,\!\?\;]', ' ', text)
        
        # Remover espacios múltiples
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def analyze_sentiment_vader(self, text):
        """Análisis de sentimiento con VADER"""
        if not self.vader_analyzer:
            return {'compound': 0.0, 'positive': 0.0, 'negative': 0.0, 'neutral': 1.0}
        
        scores = self.vader_analyzer.polarity_scores(text)
        return {
            'compound': scores['compound'],
            'positive': scores['pos'],
            'negative': scores['neg'],
            'neutral': scores['neu']
        }
    
    def analyze_sentiment_textblob(self, text):
        """Análisis de sentimiento con TextBlob"""
        try:
            blob = TextBlob(text)
            return {
                'polarity': blob.sentiment.polarity,
                'subjectivity': blob.sentiment.subjectivity
            }
        except:
            return {'polarity': 0.0, 'subjectivity': 0.0}
    
    def analyze_custom_keywords(self, text):
        """Análisis basado en palabras clave personalizadas"""
        if not text or not self.palabras_clave:
            return {
                'score': 0.0,
                'positive_words': [],
                'negative_words': [],
                'detected_keywords': []
            }
        
        words = text.lower().split()
        positive_words = []
        negative_words = []
        detected_keywords = []
        total_score = 0.0
        word_count = 0
        
        for word in words:
            # Buscar palabra exacta
            if word in self.palabras_clave:
                keyword_data = self.palabras_clave[word]
                peso = keyword_data['peso']
                total_score += peso
                word_count += 1
                
                detected_keywords.append({
                    'palabra': word,
                    'peso': peso,
                    'categoria': keyword_data['categoria']
                })
                
                if peso > 0:
                    positive_words.append(word)
                elif peso < 0:
                    negative_words.append(word)
        
        # Calcular puntuación promedio
        avg_score = total_score / word_count if word_count > 0 else 0.0
        
        return {
            'score': avg_score,
            'positive_words': positive_words,
            'negative_words': negative_words,
            'detected_keywords': detected_keywords,
            'word_count': word_count
        }
    
    def determine_emotion_category(self, combined_score):
        """Determinar categoría emocional basada en puntuación combinada"""
        if combined_score >= 0.6:
            return 1  # Muy Positivo
        elif combined_score >= 0.2:
            return 2  # Positivo
        elif combined_score >= -0.2:
            return 3  # Neutral
        elif combined_score >= -0.6:
            return 4  # Negativo
        else:
            return 5  # Muy Negativo
    
    def analyze_review_sentiment(self, review_text, review_id):
        """Análisis completo de sentimiento para una reseña"""
        if not review_text:
            return None
        
        # Limpiar texto
        clean_text = self.clean_text(review_text)
        
        # Análisis con diferentes métodos
        vader_result = self.analyze_sentiment_vader(clean_text)
        textblob_result = self.analyze_sentiment_textblob(clean_text)
        custom_result = self.analyze_custom_keywords(clean_text)
        
        # Combinar puntuaciones (promedio ponderado)
        combined_score = (
            vader_result['compound'] * 0.4 +
            textblob_result['polarity'] * 0.3 +
            custom_result['score'] * 0.3
        )
        
        # Determinar categoría emocional
        categoria_id = self.determine_emotion_category(combined_score)
        
        # Calcular confianza basada en consistencia entre métodos
        scores = [vader_result['compound'], textblob_result['polarity'], custom_result['score']]
        variance = sum((x - combined_score) ** 2 for x in scores) / len(scores)
        confidence = max(0.0, 1.0 - variance)
        
        return {
            'review_id': review_id,
            'categoria_emocional_id': categoria_id,
            'puntuacion_sentimiento': combined_score,
            'confianza': confidence,
            'palabras_positivas': ', '.join(custom_result['positive_words'][:10]),
            'palabras_negativas': ', '.join(custom_result['negative_words'][:10]),
            'palabras_clave_detectadas': json.dumps(custom_result['detected_keywords'][:15])
        }
    
    def save_sentiment_analysis(self, analysis_result):
        """Guardar análisis de sentimiento en la base de datos"""
        try:
            cursor = self.db.connection.cursor()
            
            query = """
            INSERT INTO AnalisisSentimientos 
            (review_id, categoria_emocional_id, puntuacion_sentimiento, confianza, 
             palabras_positivas, palabras_negativas, palabras_clave_detectadas)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(query, (
                analysis_result['review_id'],
                analysis_result['categoria_emocional_id'],
                analysis_result['puntuacion_sentimiento'],
                analysis_result['confianza'],
                analysis_result['palabras_positivas'],
                analysis_result['palabras_negativas'],
                analysis_result['palabras_clave_detectadas']
            ))
            
            self.db.connection.commit()
            return True
            
        except Exception as e:
            print(f"❌ Error guardando análisis de sentimiento: {str(e)}")
            return False
    
    def analyze_all_reviews_for_sucursal(self, sucursal_id):
        """Analizar todas las reseñas de una sucursal"""
        try:
            cursor = self.db.connection.cursor()
            
            # CONSULTA CORREGIDA para NVARCHAR(MAX)
            query = """
            SELECT r.id, r.texto 
            FROM Reviews r 
            LEFT JOIN AnalisisSentimientos a ON r.id = a.review_id
            WHERE r.sucursal_id = ? 
            AND a.id IS NULL 
            AND r.texto IS NOT NULL 
            AND LEN(r.texto) > 0
            """
            
            cursor.execute(query, (sucursal_id,))
            reviews = cursor.fetchall()
            
            analyzed_count = 0
            
            print(f"📝 Encontrados {len(reviews)} reviews para analizar en sucursal {sucursal_id}")
            
            for review in reviews:
                review_id, texto = review
                
                if texto and texto.strip():  # Verificar que no esté vacío
                    # Analizar sentimiento
                    analysis = self.analyze_review_sentiment(texto, review_id)
                    
                    if analysis and self.save_sentiment_analysis(analysis):
                        analyzed_count += 1
                        print(f"✅ Review {review_id} analizado")
                    else:
                        print(f"❌ Error analizando review {review_id}")
                else:
                    print(f"⚠️ Review {review_id} sin texto válido")
            
            print(f"📊 {analyzed_count} reviews analizados para sucursal {sucursal_id}")
            return analyzed_count
            
        except Exception as e:
            print(f"❌ Error analizando reviews de sucursal {sucursal_id}: {str(e)}")
            return 0
        
        
    def calculate_emotional_metrics(self, sucursal_id):
        """Calcular métricas emocionales para una sucursal"""
        try:
            cursor = self.db.connection.cursor()
            
            # Obtener estadísticas de sentimientos
            query = """
            SELECT 
                ce.nombre,
                COUNT(a.id) as cantidad,
                AVG(a.puntuacion_sentimiento) as promedio_puntuacion
            FROM AnalisisSentimientos a
            INNER JOIN Reviews r ON a.review_id = r.id
            INNER JOIN CategoriasEmocionales ce ON a.categoria_emocional_id = ce.id
            WHERE r.sucursal_id = ?
            GROUP BY ce.id, ce.nombre
            """
            
            cursor.execute(query, (sucursal_id,))
            results = cursor.fetchall()
            
            # Calcular totales y porcentajes
            total_reviews = sum(row.cantidad for row in results)
            
            if total_reviews == 0:
                print(f"⚠️ No hay reviews analizados para calcular métricas de sucursal {sucursal_id}")
                return None
            
            metrics = {
                'total_reviews_analizados': total_reviews,
                'porcentaje_muy_positivo': 0,
                'porcentaje_positivo': 0,
                'porcentaje_neutral': 0,
                'porcentaje_negativo': 0,
                'porcentaje_muy_negativo': 0,
                'puntuacion_promedio_sentimiento': 0
            }
            
            total_puntuacion = 0
            
            for row in results:
                categoria = row.nombre
                cantidad = row.cantidad
                promedio = row.promedio_puntuacion or 0
                
                porcentaje = (cantidad / total_reviews) * 100
                total_puntuacion += promedio * cantidad
                
                if categoria == 'Muy Positivo':
                    metrics['porcentaje_muy_positivo'] = porcentaje
                elif categoria == 'Positivo':
                    metrics['porcentaje_positivo'] = porcentaje
                elif categoria == 'Neutral':
                    metrics['porcentaje_neutral'] = porcentaje
                elif categoria == 'Negativo':
                    metrics['porcentaje_negativo'] = porcentaje
                elif categoria == 'Muy Negativo':
                    metrics['porcentaje_muy_negativo'] = porcentaje
            
            # Puntuación promedio general
            metrics['puntuacion_promedio_sentimiento'] = total_puntuacion / total_reviews
            
            # Índice de satisfacción (0-100)
            satisfaccion = (
                metrics['porcentaje_muy_positivo'] * 1.0 +
                metrics['porcentaje_positivo'] * 0.75 +
                metrics['porcentaje_neutral'] * 0.5 +
                metrics['porcentaje_negativo'] * 0.25 +
                metrics['porcentaje_muy_negativo'] * 0.0
            )
            metrics['indice_satisfaccion'] = satisfaccion
            
            # Palabras más mencionadas
            palabras_query = """
            SELECT palabras_clave_detectadas
            FROM AnalisisSentimientos a
            INNER JOIN Reviews r ON a.review_id = r.id
            WHERE r.sucursal_id = ? AND palabras_clave_detectadas IS NOT NULL
            """
            
            cursor.execute(palabras_query, (sucursal_id,))
            palabras_results = cursor.fetchall()
            
            all_keywords = []
            for row in palabras_results:
                try:
                    keywords = json.loads(row.palabras_clave_detectadas)
                    for kw in keywords:
                        all_keywords.append(kw['palabra'])
                except:
                    continue
            
            # Top 10 palabras más mencionadas
            if all_keywords:
                word_counts = Counter(all_keywords)
                top_words = [f"{word}({count})" for word, count in word_counts.most_common(10)]
                metrics['palabras_mas_mencionadas'] = ', '.join(top_words)
            else:
                metrics['palabras_mas_mencionadas'] = ''
            
            return metrics
            
        except Exception as e:
            print(f"❌ Error calculando métricas emocionales: {str(e)}")
            return None

    def save_emotional_metrics(self, sucursal_id, metrics):
        """Guardar métricas emocionales en la base de datos"""
        try:
            cursor = self.db.connection.cursor()
            
            # Verificar si ya existen métricas para esta sucursal
            check_query = "SELECT id FROM MetricasEmocionales WHERE sucursal_id = ?"
            cursor.execute(check_query, (sucursal_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Actualizar métricas existentes
                query = """
                UPDATE MetricasEmocionales SET
                    total_reviews_analizados = ?,
                    porcentaje_muy_positivo = ?,
                    porcentaje_positivo = ?,
                    porcentaje_neutral = ?,
                    porcentaje_negativo = ?,
                    porcentaje_muy_negativo = ?,
                    puntuacion_promedio_sentimiento = ?,
                    indice_satisfaccion = ?,
                    palabras_mas_mencionadas = ?,
                    fecha_ultimo_analisis = GETDATE()
                WHERE sucursal_id = ?
                """
                
                cursor.execute(query, (
                    metrics['total_reviews_analizados'],
                    metrics['porcentaje_muy_positivo'],
                    metrics['porcentaje_positivo'],
                    metrics['porcentaje_neutral'],
                    metrics['porcentaje_negativo'],
                    metrics['porcentaje_muy_negativo'],
                    metrics['puntuacion_promedio_sentimiento'],
                    metrics['indice_satisfaccion'],
                    metrics['palabras_mas_mencionadas'],
                    sucursal_id
                ))
            else:
                # Insertar nuevas métricas
                query = """
                INSERT INTO MetricasEmocionales 
                (sucursal_id, total_reviews_analizados, porcentaje_muy_positivo, 
                porcentaje_positivo, porcentaje_neutral, porcentaje_negativo, 
                porcentaje_muy_negativo, puntuacion_promedio_sentimiento, 
                indice_satisfaccion, palabras_mas_mencionadas)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(query, (
                    sucursal_id,
                    metrics['total_reviews_analizados'],
                    metrics['porcentaje_muy_positivo'],
                    metrics['porcentaje_positivo'],
                    metrics['porcentaje_neutral'],
                    metrics['porcentaje_negativo'],
                    metrics['porcentaje_muy_negativo'],
                    metrics['puntuacion_promedio_sentimiento'],
                    metrics['indice_satisfaccion'],
                    metrics['palabras_mas_mencionadas']
                ))
            
            self.db.connection.commit()
            print(f"✅ Métricas emocionales guardadas para sucursal {sucursal_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando métricas emocionales: {str(e)}")
            return False


# Función para analizar sentimientos de todas las sucursales
def analyze_all_sentiments(db_manager):
    """Analizar sentimientos de todas las sucursales"""
    analyzer = SentimentAnalyzer(db_manager)
    
    try:
        cursor = db_manager.connection.cursor()
        
        # Obtener todas las sucursales
        cursor.execute("SELECT id, nombre FROM Sucursales")
        sucursales = cursor.fetchall()
        
        print(f"🧠 Iniciando análisis de sentimientos para {len(sucursales)} sucursales")
        print("=" * 60)
        
        total_analyzed = 0
        
        for sucursal in sucursales:
            sucursal_id, nombre = sucursal
            
            print(f"\n🔍 Analizando: {nombre}")
            print("-" * 40)
            
            # Analizar reviews de la sucursal
            analyzed_count = analyzer.analyze_all_reviews_for_sucursal(sucursal_id)
            total_analyzed += analyzed_count
            
            if analyzed_count > 0:
                # Calcular métricas emocionales
                metrics = analyzer.calculate_emotional_metrics(sucursal_id)
                
                if metrics:
                    analyzer.save_emotional_metrics(sucursal_id, metrics)
                    
                    # Mostrar resumen
                    print(f"📊 Resumen emocional:")
                    print(f"   • Muy Positivo: {metrics['porcentaje_muy_positivo']:.1f}%")
                    print(f"   • Positivo: {metrics['porcentaje_positivo']:.1f}%")
                    print(f"   • Neutral: {metrics['porcentaje_neutral']:.1f}%")
                    print(f"   • Negativo: {metrics['porcentaje_negativo']:.1f}%")
                    print(f"   • Muy Negativo: {metrics['porcentaje_muy_negativo']:.1f}%")
                    print(f"   • Índice de Satisfacción: {metrics['indice_satisfaccion']:.1f}/100")
                    print(f"   • Puntuación Promedio: {metrics['puntuacion_promedio_sentimiento']:.3f}")
                else:
                    print("❌ Error calculando métricas emocionales")
            else:
                print("   ⚠️ No hay reviews nuevos para analizar")
        
        print(f"\n🎉 Análisis completado!")
        print(f"📈 Total de reviews analizados: {total_analyzed}")
        
    except Exception as e:
        print(f"❌ Error en análisis general: {str(e)}")





def clean_urls_file(filename):
    """Limpiar URLs duplicadas del archivo"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        seen_urls = set()
        clean_lines = []
        
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                if line not in seen_urls:
                    seen_urls.add(line)
                    clean_lines.append(line)
                else:
                    print(f"⚠️ URL duplicada removida: {line}")
            elif line.startswith('#') or line == '':
                clean_lines.append(line)
        
        # Escribir archivo limpio
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(clean_lines))
        
        print(f"✅ Archivo {filename} limpiado. URLs únicas: {len(seen_urls)}")
        return list(seen_urls)
        
    except Exception as e:
        print(f"❌ Error limpiando archivo: {str(e)}")
        return []

def read_urls_from_file(filename):
    """Lee las URLs desde un archivo de texto y elimina duplicados"""
    try:
        # Primero limpiar duplicados
        urls = clean_urls_file(filename)
        return urls
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {filename}")
        return []
    except Exception as e:
        print(f"Error leyendo el archivo {filename}: {str(e)}")
        return []


def main():
    """Función principal que procesa todas las URLs y analiza sentimientos"""
    urls_file = 'urls.txt'
    
    # Conectar a la base de datos
    db = DatabaseManager(DB_CONFIG)
    if not db.connect():
        print("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
        return
    
    try:
        # Leer URLs del archivo (ya limpia duplicados automáticamente)
        urls = read_urls_from_file(urls_file)
        
        if not urls:
            print(f"No se encontraron URLs en {urls_file}")
            return
        
        print(f"📋 Se encontraron {len(urls)} URLs únicas para procesar")
        print("=" * 50)
        
        # Procesar cada URL
        successful_extractions = 0
        failed_extractions = 0
        successful_db_saves = 0
        
        for i, url in enumerate(urls, 1):
            print(f"\n🔄 Procesando URL {i}/{len(urls)}")
            print(f"URL: {url}")
            print("-" * 30)
            
            try:
                resultado = scrape_google_maps(url)
                
                if resultado:
                    # Guardar en base de datos
                    if db.save_complete_data(resultado):
                        successful_db_saves += 1
                        print("💾 Datos guardados en SQL Server exitosamente")
                    
                    # Guardar backup en JSON (opcional)
                    save_result_to_json(resultado, i)
                    successful_extractions += 1
                    
                    # Mostrar resumen
                    print(f"📍 Nombre: {resultado['nombre']}")
                    print(f"📍 Ubicación: {resultado['ubicacion']}")
                    print(f"⭐ Rating global: {resultado['rating_global']}")
                    print(f"💬 Total reviews: {resultado['total_reviews']}")
                    print(f"📝 Reviews extraídos: {len(resultado['reviews'])}")
                else:
                    print(f"❌ No se pudieron extraer los datos de la URL {i}")
                    failed_extractions += 1
                    
            except Exception as e:
                print(f"❌ Error procesando URL {i}: {str(e)}")
                failed_extractions += 1
            
            # Pausa entre extracciones
            if i < len(urls):
                print("⏳ Esperando 3 segundos antes de la siguiente extracción...")
                time.sleep(3)
        
        # Mostrar resumen de extracción
        print("\n" + "=" * 50)
        print("📊 RESUMEN DE EXTRACCIÓN")
        print("=" * 50)
        print(f"✅ Extracciones exitosas: {successful_extractions}")
        print(f"💾 Guardados en BD exitosos: {successful_db_saves}")
        print(f"❌ Extracciones fallidas: {failed_extractions}")
        
        # ANÁLISIS DE SENTIMIENTOS
        if successful_db_saves > 0:
            print(f"\n🧠 INICIANDO ANÁLISIS DE SENTIMIENTOS")
            print("=" * 50)
            
            # Preguntar al usuario si quiere hacer análisis de sentimientos
            response = input("¿Deseas realizar análisis de sentimientos? (s/n): ").lower().strip()
            
            if response in ['s', 'si', 'sí', 'y', 'yes']:
                analyze_all_sentiments(db)
                
                # Mostrar estadísticas finales
                print_final_statistics(db)
            else:
                print("⏭️ Análisis de sentimientos omitido")
        
        print(f"\n🎉 Proceso completado!")
        
    except Exception as e:
        print(f"❌ Error en función main: {str(e)}")
        
    finally:
        db.disconnect()

def print_final_statistics(db_manager):
    """Mostrar estadísticas finales del análisis"""
    try:
        cursor = db_manager.connection.cursor()
        
        print(f"\n📈 ESTADÍSTICAS FINALES")
        print("=" * 50)
        
        # Estadísticas generales
        stats_query = """
        SELECT 
            COUNT(DISTINCT s.id) as total_sucursales,
            COUNT(DISTINCT r.id) as total_reviews,
            COUNT(DISTINCT a.id) as total_reviews_analizados
        FROM Sucursales s
        LEFT JOIN Reviews r ON s.id = r.sucursal_id
        LEFT JOIN AnalisisSentimientos a ON r.id = a.review_id
        """
        
        cursor.execute(stats_query)
        stats = cursor.fetchone()
        
        print(f"🏢 Total de sucursales: {stats.total_sucursales}")
        print(f"💬 Total de reviews: {stats.total_reviews}")
        print(f"🧠 Reviews analizados: {stats.total_reviews_analizados}")
        
        # Distribución emocional general
        emotion_query = """
        SELECT 
            ce.nombre,
            COUNT(a.id) as cantidad,
            CAST(COUNT(a.id) * 100.0 / SUM(COUNT(a.id)) OVER() AS DECIMAL(5,2)) as porcentaje
        FROM AnalisisSentimientos a
        INNER JOIN CategoriasEmocionales ce ON a.categoria_emocional_id = ce.id
        GROUP BY ce.id, ce.nombre
        ORDER BY ce.id
        """
        
        cursor.execute(emotion_query)
        emotions = cursor.fetchall()
        
        if emotions:
            print(f"\n🎭 DISTRIBUCIÓN EMOCIONAL GENERAL:")
            print("-" * 50)
            for emotion in emotions:
                emoji = get_emotion_emoji(emotion.nombre)
                print(f"{emoji} {emotion.nombre}: {emotion.cantidad} reviews ({emotion.porcentaje}%)")
    
    except Exception as e:
        print(f"❌ Error mostrando estadísticas: {str(e)}")

def get_emotion_emoji(emotion_name):
    """Obtener emoji según la emoción"""
    emojis = {
        'Muy Positivo': '😍',
        'Positivo': '😊',
        'Neutral': '😐',
        'Negativo': '😞',
        'Muy Negativo': '😡'
    }
    return emojis.get(emotion_name, '❓')

def create_example_urls_file():
    """Crea un archivo urls.txt de ejemplo si no existe"""
    if not os.path.exists('urls.txt'):
        example_urls = [
            "https://maps.app.goo.gl/3YoJDkZKQQ1HkznW7",
            "# Agrega más URLs aquí, una por línea",
            "# Las líneas que empiecen con # serán ignoradas"
        ]
        
        try:
            with open('urls.txt', 'w', encoding='utf-8') as f:
                f.write('\n'.join(example_urls))
            print("📝 Se creó un archivo urls.txt de ejemplo")
            print("   Edita este archivo y agrega tus URLs de Google Maps")
            return True
        except Exception as e:
            print(f"❌ Error creando urls.txt: {str(e)}")
            return False
    return False

def analyze_single_sucursal(db_manager, sucursal_id):
    """Analizar sentimientos de una sucursal específica"""
    analyzer = SentimentAnalyzer(db_manager)
    
    try:
        cursor = db_manager.connection.cursor()
        cursor.execute("SELECT nombre FROM Sucursales WHERE id = ?", (sucursal_id,))
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ No se encontró sucursal con ID {sucursal_id}")
            return
        
        nombre = result.nombre
        print(f"🔍 Analizando sentimientos para: {nombre}")
        
        # Analizar reviews
        analyzed_count = analyzer.analyze_all_reviews_for_sucursal(sucursal_id)
        
        if analyzed_count > 0:
            print(f"✅ Análisis completado para {nombre}")
        else:
            print(f"⚠️ No hay reviews nuevos para analizar en {nombre}")
    
    except Exception as e:
        print(f"❌ Error analizando sucursal {sucursal_id}: {str(e)}")

# Ejemplo de uso
if __name__ == "__main__":
    print("🗺️  EXTRACTOR DE RESEÑAS DE GOOGLE MAPS CON ANÁLISIS DE SENTIMIENTOS")
    print("=" * 70)
    
    # Crear archivo de ejemplo si no existe
    if create_example_urls_file():
        print("Por favor, edita el archivo urls.txt con tus URLs y ejecuta el script nuevamente.")
        print("También configura los datos de conexión a SQL Server en DB_CONFIG.")
    else:
        # Ejecutar el proceso principal
        main()
