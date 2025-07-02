from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tabla_pagos_ap():
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        sql_script = """
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
        """

        with engine.connect() as conn:
            conn.execute(text(sql_script))
            conn.commit()

        print("✅ Tabla 'pagos_ap' creada exitosamente.")

    except Exception as e:
        print(f"❌ Error al crear la tabla pagos_ap: {e}")

def crear_tabla_pagos_comparendos():
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        sql_script = """
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
        """

        with engine.connect() as conn:
            conn.execute(text(sql_script))
            conn.commit()

        print("✅ Tabla 'pagos_comparendos' creada exitosamente.")

    except Exception as e:
        print(f"❌ Error al crear la tabla pagos_comparendos: {e}")

# Ejecución directa para pruebas locales
if __name__ == "__main__":
    crear_tabla_pagos_ap()
    crear_tabla_pagos_comparendos()
