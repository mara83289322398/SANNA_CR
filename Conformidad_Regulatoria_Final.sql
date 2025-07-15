CREATE DATABASE SannaIConformidadRegulatoria
go
USE SannaIConformidadRegulatoria
go

-- Tabla de dimensión: Sucursales
CREATE TABLE Sucursales (
    id INT IDENTITY(1,1) PRIMARY KEY,
    url NVARCHAR(500) NOT NULL,
    nombre NVARCHAR(255) NOT NULL,
    ubicacion NVARCHAR(500),
    sitio_web NVARCHAR(255),
    telefono NVARCHAR(50),
    referencia NVARCHAR(100),
    fecha_extraccion DATETIME DEFAULT GETDATE(),
    UNIQUE(url)
)
go

-- Tabla de dimensión: Usuarios
CREATE TABLE Usuarios(
    id_usuario NVARCHAR(8) PRIMARY KEY NOT NULL,
    nombre_usuario NVARCHAR(100) NOT NULL,
    apellido_usuario NVARCHAR(100) NOT NULL,
    tipopersona_usuario NVARCHAR(100) NOT NULL
)
go

-- Tabla de dimensión: Tiempo
CREATE TABLE Tiempo(
    id_tiempo INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    año_mes NCHAR(61) NOT NULL,
    dia_semana NCHAR(30) NOT NULL,
    trimestre INT NOT NULL,
    dia_año INT NOT NULL,
    semana_año INT NOT NULL,
    mes INT NOT NULL,
    año INT NOT NULL,
    fecha DATETIME NOT NULL UNIQUE
)
go

-- Tabla de dimensión: Normativas
CREATE TABLE Normativas(
    id_normativa NVARCHAR(8) PRIMARY KEY NOT NULL,
    tipo_normativa NVARCHAR(100) NOT NULL,
    estado_normativa NVARCHAR(50) NOT NULL,
    resultado_normativa NVARCHAR(100) NOT NULL,
    acciones_normativa NVARCHAR(300) NOT NULL,
    sucursal_id INT NOT NULL,
    id_usuario NVARCHAR(8),
    fecha DATETIME,
    id_tiempo INT NULL,
    FOREIGN KEY (sucursal_id) REFERENCES Sucursales(id) ON DELETE CASCADE,
    FOREIGN KEY (id_usuario) REFERENCES Usuarios(id_usuario),
    FOREIGN KEY (id_tiempo) REFERENCES Tiempo(id_tiempo)
)
go

-- Tabla de hechos: Hechos_Conformidad_Sanitaria
CREATE TABLE Hechos_Conformidad_Sanitaria(
    sucursal_id INT NOT NULL,
    id_usuario NVARCHAR(8) NOT NULL,
    id_normativa NVARCHAR(8) NOT NULL,
    fecha DATETIME NOT NULL,
    total_acciones_correctivas INT NOT NULL,
    total_acciones_correctivas_conformes INT NOT NULL,
    total_acciones_correctivas_noconformes INT NOT NULL,
    FOREIGN KEY (sucursal_id) REFERENCES Sucursales(id),
    FOREIGN KEY (id_usuario) REFERENCES Usuarios(id_usuario),
    FOREIGN KEY (id_normativa) REFERENCES Normativas(id_normativa),
    FOREIGN KEY (fecha) REFERENCES Tiempo(fecha)
)
go

CREATE TABLE Calificaciones (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sucursal_id INT NOT NULL,
    rating_global DECIMAL(3,2),
    total_reviews INT DEFAULT 0,
    fecha_calificacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (sucursal_id) REFERENCES Sucursales(id) ON DELETE CASCADE
);
go

CREATE TABLE Horarios (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sucursal_id INT NOT NULL,
    dia_semana NVARCHAR(20) NOT NULL,
    horas NVARCHAR(50),
    esta_cerrado BIT DEFAULT 0,
    FOREIGN KEY (sucursal_id) REFERENCES Sucursales(id) ON DELETE CASCADE
);
go

CREATE TABLE Reviews (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sucursal_id INT NOT NULL,
    autor NVARCHAR(255),
    rating INT,
    fecha_review NVARCHAR(50),
    texto NVARCHAR(MAX),
    cantidad_fotos INT DEFAULT 0,
    likes INT DEFAULT 0,
    fecha_extraccion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (sucursal_id) REFERENCES Sucursales(id) ON DELETE CASCADE
);
go

CREATE TABLE CategoriasEmocionales (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nombre NVARCHAR(50) NOT NULL UNIQUE,
    descripcion NVARCHAR(255),
    color_hex NVARCHAR(7)
);
go

CREATE TABLE PalabrasClave (
    id INT IDENTITY(1,1) PRIMARY KEY,
    palabra NVARCHAR(100) NOT NULL,
    categoria_emocional_id INT NOT NULL,
    peso DECIMAL(3,2) DEFAULT 1.0,
    tipo NVARCHAR(20) DEFAULT 'general',
    FOREIGN KEY (categoria_emocional_id) REFERENCES CategoriasEmocionales(id)
);
go

CREATE TABLE AnalisisSentimientos (
    id INT IDENTITY(1,1) PRIMARY KEY,
    review_id INT NOT NULL,
    categoria_emocional_id INT NOT NULL,
    puntuacion_sentimiento DECIMAL(5,4),
    confianza DECIMAL(5,4),
    palabras_positivas NVARCHAR(500),
    palabras_negativas NVARCHAR(500),
    palabras_clave_detectadas NVARCHAR(1000),
    fecha_analisis DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (review_id) REFERENCES Reviews(id) ON DELETE CASCADE,
    FOREIGN KEY (categoria_emocional_id) REFERENCES CategoriasEmocionales(id)
);
go

CREATE TABLE MetricasEmocionales (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sucursal_id INT NOT NULL,
    total_reviews_analizados INT DEFAULT 0,
    porcentaje_muy_positivo DECIMAL(5,2) DEFAULT 0,
    porcentaje_positivo DECIMAL(5,2) DEFAULT 0,
    porcentaje_neutral DECIMAL(5,2) DEFAULT 0,
    porcentaje_negativo DECIMAL(5,2) DEFAULT 0,
    porcentaje_muy_negativo DECIMAL(5,2) DEFAULT 0,
    puntuacion_promedio_sentimiento DECIMAL(5,4),
    indice_satisfaccion DECIMAL(5,2),
    palabras_mas_mencionadas NVARCHAR(1000),
    fecha_ultimo_analisis DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (sucursal_id) REFERENCES Sucursales(id) ON DELETE CASCADE
);
go

CREATE INDEX IX_Reviews_SucursalId ON Reviews(sucursal_id);
go
CREATE INDEX IX_Reviews_Rating ON Reviews(rating);
go
CREATE INDEX IX_Horarios_SucursalId ON Horarios(sucursal_id);
go
CREATE INDEX IX_Calificaciones_SucursalId ON Calificaciones(sucursal_id);
go
CREATE INDEX IX_AnalisisSentimientos_ReviewId ON AnalisisSentimientos(review_id);
go
CREATE INDEX IX_AnalisisSentimientos_Categoria ON AnalisisSentimientos(categoria_emocional_id);
go
CREATE INDEX IX_MetricasEmocionales_SucursalId ON MetricasEmocionales(sucursal_id);
go



select * from Sucursales;
select * from Normativas;
select * from Usuarios;
select * from Tiempo;
select * from Hechos_Conformidad_Sanitaria;

