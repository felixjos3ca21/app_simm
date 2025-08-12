from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tablas():
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        scripts = {
            'pagos_ap': """
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
            """,
            'pagos_comparendos': """
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
            """,
            'archivos_procesados_pagos': """
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
            """
        }

        with engine.connect() as conn:
            for nombre, script in scripts.items():
                conn.execute(text(script))
                conn.commit()
                print(f"✅ Tabla '{nombre}' creada/verificada exitosamente.")

    except Exception as e:
        print(f"❌ Error al crear las tablas: {e}")

if __name__ == "__main__":
    crear_tablas()
