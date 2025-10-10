#!/usr/bin/env python3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.environ['DB_SIMM_HOST'],
    'database': os.environ['DB_SIMM_NAME'],
    'user': os.environ['DB_SIMM_USER'],
    'password': os.environ['DB_SIMM_PASSWORD'],
    'port': os.environ['DB_SIMM_PORT']
}

connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(connection_string)

tables = ['archivos_procesados_tip3', 'archivos_procesados_cdr5', 'archivos_procesados_camp_3']

for table in tables:
    print(f"=== {table} ===")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position"))
            columns = [row[0] for row in result]
            print("Columnas:", ', '.join(columns))
            
            # Verificar si tiene error_message
            if 'error_message' in columns:
                print("✅ Tiene error_message")
            else:
                print("❌ NO tiene error_message")
    except Exception as e:
        print(f"Error: {e}")
    print()
