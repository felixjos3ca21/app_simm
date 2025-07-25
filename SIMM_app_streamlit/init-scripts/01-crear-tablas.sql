-- Script de inicialización para crear todas las tablas automáticamente
-- Este archivo se ejecuta cuando PostgreSQL se inicia por primera vez

-- Crear tabla BASES
CREATE TABLE IF NOT EXISTS bases (
    id_registro VARCHAR(75),
    base VARCHAR(100) NOT NULL,
    fecha_entrega DATE NOT NULL,
    tipo_documento VARCHAR(50),
    documento VARCHAR(50),
    nombre VARCHAR(200),
    numero_comparendo VARCHAR(75),
    fecha_comparendo DATE,
    codigo_infraccion VARCHAR(20),
    placa VARCHAR(20),
    valor_infraccion NUMERIC(16, 2),
    telefono VARCHAR(30),
    archivo_origen VARCHAR(100) NOT NULL,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estado_telefono VARCHAR(60)
);

-- Crear tabla GESTIONES
CREATE TABLE IF NOT EXISTS gestiones (
    id_registro VARCHAR(64) PRIMARY KEY,
    id_gestion_Campaña VARCHAR(50),
    tipo_documento VARCHAR(50),
    documento VARCHAR(30) NOT NULL,
    nombre_usuario VARCHAR(100),
    fecha_gestion TIMESTAMP NOT NULL,
    tipo_llamada VARCHAR(50),
    id_gestion VARCHAR(50) NOT NULL,
    resultado VARCHAR(100) NOT NULL,
    fecha_compromiso DATE,
    asesor VARCHAR(100),
    campana VARCHAR(50) NOT NULL,
    telefono VARCHAR(20),
    obligacion VARCHAR(50),
    numero_comparendo VARCHAR(50),
    valor NUMERIC(12, 2),
    identificador_infraccion VARCHAR(50),
    archivo_origen VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_gestion_sencilla TIMESTAMP,
    tipo_chat VARCHAR(50)
);

-- Crear tabla PAGOS
CREATE TABLE IF NOT EXISTS pagos (
    id_registro VARCHAR(64),
    nro_acuerdo VARCHAR(20),
    nro_comparendo VARCHAR(60),
    documento VARCHAR(20) NOT NULL,
    nombre_usuario VARCHAR(100) NOT NULL,
    valor NUMERIC(15,2) NOT NULL,
    fecha_pago TIMESTAMP NOT NULL,
    archivo_origen VARCHAR(75) NOT NULL,
    identificador_infraccion VARCHAR(60),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear tabla SMS
CREATE TABLE IF NOT EXISTS SMS (
    id_registro VARCHAR(64) PRIMARY KEY,
    tipo_documento VARCHAR(50),
    documento VARCHAR(30),
    nombre_usuario VARCHAR(100),
    fecha_SMS DATE NOT NULL,
    resultado VARCHAR(100),
    texto_sms VARCHAR(250),
    base VARCHAR(50),
    telefono VARCHAR(20) NOT NULL,
    numero_comparendo VARCHAR(50),
    identificador_infraccion VARCHAR(50),
    archivo_origen VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear tabla PAGOS_AP
CREATE TABLE IF NOT EXISTS pagos_ap (
    nro_acuerdo VARCHAR(50),
    id_usuario VARCHAR(20),
    valor NUMERIC(15, 2),
    fecha_liquida TIMESTAMP,
    consecutivo_cuota VARCHAR(50),
    nombre_usuario VARCHAR(150),
    documento VARCHAR(30),
    archivo_origen VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tipo_pago VARCHAR(10),
    id_registro VARCHAR(64) PRIMARY KEY
);

-- Crear tabla PAGOS_COMPARENDOS
CREATE TABLE IF NOT EXISTS pagos_comparendos (
    nro_comparendo VARCHAR(60),
    nro_recibo VARCHAR(30),
    fecha_liquida TIMESTAMP,
    compute_0004 NUMERIC(15, 2),
    id_usuario VARCHAR(20),
    nro_resolucion VARCHAR(50),
    intereses NUMERIC(15, 2),
    nombre_usuario VARCHAR(150),
    valor NUMERIC(15, 2),
    documento VARCHAR(30),
    archivo_origen VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tipo_pago VARCHAR(10),
    id_registro VARCHAR(64) PRIMARY KEY
);

-- Crear tabla ARCHIVOS_PROCESADOS_PAGOS
CREATE TABLE IF NOT EXISTS archivos_procesados_pagos (
    id SERIAL PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL,
    ruta_archivo VARCHAR(500) NOT NULL,
    tipo_archivo VARCHAR(20) NOT NULL,
    fecha_procesamiento TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    registros_procesados INTEGER NOT NULL,
    estado VARCHAR(20) NOT NULL,
    hash_archivo VARCHAR(32) NOT NULL,
    UNIQUE(ruta_archivo)
);

-- Crear tabla ARCHIVOS_PROCESADOS_PAGOS
CREATE TABLE IF NOT EXISTS archivos_procesados_pagos (
    id SERIAL PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL,
    ruta_archivo VARCHAR(500) NOT NULL,
    tipo_archivo VARCHAR(20) NOT NULL,
    fecha_procesamiento TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    registros_procesados INTEGER NOT NULL,
    estado VARCHAR(20) NOT NULL,
    hash_archivo VARCHAR(32) NOT NULL,
    UNIQUE(ruta_archivo)
);

CREATE TABLE IF NOT EXISTS tipificaciones_3 (
                conn_id TEXT,
                agent_id TEXT,
                agent_name TEXT,
                agent_dni TEXT,
                skill_id TEXT,
                date TIMESTAMP,  
                cod_act TEXT,
                description_cod_act TEXT,
                cod_act_2 TEXT,
                description_cod_act_2 TEXT,
                comments TEXT,
                type_interaction TEXT,
                telephone TEXT,
                destiny TEXT,
                time TIME,  
                hang_up TEXT,
                customer_id TEXT,
                campaign_id TEXT,
                archivo_origen TEXT,
                module TEXT
);

CREATE TABLE IF NOT EXISTS archivos_procesados_tip3 (
                id SERIAL PRIMARY KEY,
                nombre_archivo VARCHAR(255) UNIQUE NOT NULL,
                fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                registros_insertados INTEGER,
                estado VARCHAR(20),
                intentos INTEGER DEFAULT 0,
                error_message TEXT
);

CREATE TABLE IF NOT EXISTS campanas_3 (
                campaign_id TEXT, 
                customer_name TEXT, 
                customer_last_name TEXT,
                id_type TEXT, 
                customer_id TEXT, 
                date TIMESTAMP,
                telephone TEXT, 
                result TEXT, 
                opt1 TEXT, 
                opt2 TEXT,
                opt3 TEXT, 
                opt4 TEXT,
                opt5 TEXT, 
                opt6 TEXT, 
                opt7 TEXT, 
                opt8 TEXT, 
                opt9 TEXT, 
                opt10 TEXT,
                opt11 TEXT, 
                opt12 TEXT, 
                conn_id TEXT, 
                Module TEXT,
                archivo_origen TEXT
            );

CREATE TABLE IF NOT EXISTS archivos_procesados_campanas_3 (
                    id SERIAL PRIMARY KEY,
                    nombre_archivo VARCHAR(255) UNIQUE NOT NULL,
                    fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    registros_insertados INTEGER,
                    estado VARCHAR(20) DEFAULT 'completado',
                    intentos INTEGER DEFAULT 1,
                    error_message TEXT
                )



-- Crear vista materializada si no existe
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_id_gestiones AS
SELECT DISTINCT documento, numero_comparendo
FROM gestiones
WHERE documento IS NOT NULL AND numero_comparendo IS NOT NULL;

-- Crear índices para optimizar rendimiento
CREATE INDEX IF NOT EXISTS idx_bases_documento ON bases(documento);
CREATE INDEX IF NOT EXISTS idx_bases_numero_comparendo ON bases(numero_comparendo);
CREATE INDEX IF NOT EXISTS idx_gestiones_documento ON gestiones(documento);
CREATE INDEX IF NOT EXISTS idx_gestiones_numero_comparendo ON gestiones(numero_comparendo);
CREATE INDEX IF NOT EXISTS idx_pagos_documento ON pagos(documento);
CREATE INDEX IF NOT EXISTS idx_pagos_numero_comparendo ON pagos(nro_comparendo);

-- Mensaje de confirmación
DO $$
BEGIN
    RAISE NOTICE 'Todas las tablas han sido creadas exitosamente en la base de datos %', current_database();
END
$$;