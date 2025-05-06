from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tabla_pagos():
    try:
        # Conexión a PostgreSQL
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        sql_script = """
        CREATE TABLE IF NOT EXISTS pagos (
            id_registro VARCHAR(64) PRIMARY KEY,          -- Hash único
            nro_acuerdo VARCHAR(20),                      
            nro_comparendo VARCHAR(50),                   
            documento VARCHAR(20) NOT NULL,               -- Nuevo nombre (ex id_usuario)
            nombre_usuario VARCHAR(50) NOT NULL,          -- Nuevo nombre (ex nombres)
            valor NUMERIC(15,2) NOT NULL,                 
            fecha_pago TIMESTAMP NOT NULL,                
            archivo_origen VARCHAR(60) NOT NULL,          
            identificador_infraccion VARCHAR(50),         -- Campo calculado
            fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""

        # Ejecutar el script
        with engine.connect() as conn:
            conn.execute(text(sql_script))
            conn.commit()

        print("✅ Tabla 'pagos' creada exitosamente")

    except Exception as e:
        print(f"❌ Error al crear la tabla: {e}")

if __name__ == "__main__":
    crear_tabla_pagos()