import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

#config

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

#scrpt de limpieza en SQL

CLEAN_QUERY = """
DROP TABLE IF EXISTS denue_clean;

CREATE TABLE denue_clean AS
SELECT
    --Identificadores
    id,

    --Nombre del establecimiento: limpia espacios y convierte a título
    INITCAP(TRIM(nom_estab)) AS nom_estab,

    -- Sector económico
    codigo_act,
    INITCAP(TRIM(nombre_act)) AS nombre_act,

    --Tamaño de empresa (per_ocu es txt, hay que normalizar)
    TRIM(per_ocu) AS per_ocu,
    CASE
        WHEN per_ocu = '0 a 5 personas' THEN 'Micro'
        WHEN per_ocu = '6 a 10 personas'   THEN 'Micro'
        WHEN per_ocu = '11 a 30 personas'  THEN 'Pequeña'
        WHEN per_ocu = '31 a 50 personas'  THEN 'Pequeña'
        WHEN per_ocu = '51 a 100 personas' THEN 'Mediana'
        WHEN per_ocu = '101 a 250 personas' THEN 'Mediana'
        WHEN per_ocu = '251 y más personas' THEN 'Grande'
        ELSE 'No especificado'
    END AS tamanio_empresa,

    -- Geografía
    TRIM(entidad)    AS entidad,
    TRIM(municipio)  AS municipio,
    TRIM(localidad)  AS localidad,
    
    -- Coordenadas: filtra por rango geográfico válido para México
    CASE WHEN latitud  BETWEEN 14.0 AND 32.7 
         THEN latitud  END AS latitud,
    CASE WHEN longitud BETWEEN -118.4 AND -86.7 
         THEN longitud END AS longitud,
    
    -- Contacto digital (presencia online)
    CASE WHEN correoelec IS NOT NULL AND correoelec != '' 
         THEN TRUE ELSE FALSE END AS tiene_email,
    CASE WHEN www IS NOT NULL AND www != '' 
         THEN TRUE ELSE FALSE END AS tiene_web,
    
    -- Fecha de registro
    CASE WHEN fecha_alta ~ '^[0-9]{4}-[0-9]{2}$'
         THEN TO_DATE(fecha_alta || '-01', 'YYYY-MM-DD')
         ELSE NULL END AS fecha_alta

FROM denue_raw
WHERE
    --Filtra registros sin sector o sin ubicación
    codigo_act IS NOT NULL
    AND entidad IS NOT NULL
    AND municipio IS NOT NULL;
"""

if __name__ == "__main__":
    print("🧹 Iniciando limpieza de datos...")

    with engine.connect() as conn:
        for statement in CLEAN_QUERY.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
                conn.commit()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT entidad) as estados,
                SUM(CASE WHEN latitud IS NOT NULL THEN 1 ELSE 0 END) as con_coordenadas,
                SUM(CASE WHEN tiene_web THEN 1 ELSE 0 END) as con_web,
                SUM(CASE WHEN tiene_email THEN 1 ELSE 0 END) as con_email
            FROM denue_clean
        """))
        row = result.fetchone()
        print(f"\n✅ Tabla denue_clean creada:")
        print(f"   Total registros  : {row[0]:,}")
        print(f"   Estados          : {row[1]}")
        print(f"   Con coordenadas  : {row[2]:,}")
        print(f"   Con sitio web    : {row[3]:,}")
        print(f"   Con email        : {row[4]:,}")