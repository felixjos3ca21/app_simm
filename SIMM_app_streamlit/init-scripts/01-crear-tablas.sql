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
    estado_telefono VARCHAR(60),
    base_cleaned VARCHAR(100)
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
CREATE TABLE pagos (
    id_registro SERIAL PRIMARY KEY,
    codcliente VARCHAR(50),
    nitcliente VARCHAR(50),
    numobligacion VARCHAR(50),
    fechapago DATE,
    valorpago NUMERIC,
    base VARCHAR(100),
    fecha_gest DATE,
    aplica_pago_gestion VARCHAR(20),
    fecha_sms DATE,
    campania VARCHAR(50),
    aplica_pago_sms VARCHAR(50),
    aplicacion_final VARCHAR(50),
    fecha_sencilla DATE,
    anio VARCHAR(50),
    dia VARCHAR(50),
    mes VARCHAR(50),
    semana VARCHAR(50),
    numero_mes INTEGER,
    nombre_dia_semana VARCHAR(20),
    cruce_sms VARCHAR(50),
    estrategia VARCHAR(100),
    infraccion VARCHAR(100),
    periodo_21_20 VARCHAR(50),
    archivo_origen VARCHAR(255),
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

CREATE TABLE archivos_procesados_camp_3 (
    id INTEGER NOT NULL DEFAULT nextval('archivos_procesados_camp_3_id_seq'::regclass) PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL UNIQUE,
    fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    registros_insertados INTEGER,
    estado VARCHAR(20) DEFAULT 'completado'::VARCHAR,
    intentos INTEGER DEFAULT 1,
    error_message TEXT
);


CREATE TABLE archivos_procesados_ase2 (
    id INTEGER NOT NULL DEFAULT nextval('archivos_procesados_ase2_id_seq'::regclass) PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL UNIQUE,
    fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    registros_insertados INTEGER,
    estado VARCHAR(20) DEFAULT 'completado'::VARCHAR,
    intentos INTEGER DEFAULT 1,
    error_type VARCHAR(100),
    error_message TEXT,
    error_details TEXT,
    stack_trace TEXT,
    fecha_error TIMESTAMP,
    no_valid_records BOOLEAN DEFAULT FALSE
);


CREATE TABLE archivos_procesados_cdr5 (
    id INTEGER NOT NULL DEFAULT nextval('archivos_procesados_cdr5_id_seq'::regclass) PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL UNIQUE,
    fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    registros_insertados INTEGER,
    estado VARCHAR(20) DEFAULT 'completado'::VARCHAR,
    intentos INTEGER DEFAULT 1,
    error_message TEXT,
    error_type VARCHAR(100),
    error_details TEXT,
    stack_trace TEXT,
    fecha_error TIMESTAMP,
    no_valid_records BOOLEAN DEFAULT FALSE
);


CREATE TABLE asesor_2 (
    agent_id TEXT,
    agent_status TEXT,
    time BIGINT,
    date_ini TIMESTAMP,
    date_end TIMESTAMP,
    conn_id TEXT,
    type_interaction TEXT,
    destiny TEXT,
    telephone TEXT,
    campaign_id TEXT,
    agent_dni TEXT,
    agent_name TEXT,
    module TEXT,
    day INTEGER,
    month INTEGER,
    month_name TEXT,
    week TEXT,
    date TIMESTAMP,
    archivo_origen TEXT
);

DROP TABLE IF EXISTS cdr_5;

CREATE TABLE cdr_5 (
    agent_name TEXT,
    date TIMESTAMP,
    destiny TEXT,
    telephone TEXT,
    ring_time TEXT,
    result TEXT,
    type_interaction TEXT,
    customer_id TEXT,
    campaign_id TEXT,
    agent_id TEXT,
    conn_id TEXT,
    module TEXT,
    archivo_origen TEXT,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


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

