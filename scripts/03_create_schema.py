import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

SCHEMA_QUERY = """
-- ─── Tabla de hechos ────────────────────────────────────────
DROP TABLE IF EXISTS fact_empresas CASCADE;
CREATE TABLE fact_empresas TABLESPACE fast_disk AS
SELECT
    e.id,
    e.nom_estab,
    e.codigo_act                        AS sector_id,
    g.geo_id,
    t.tamanio_id,
    e.fecha_alta                        AS fecha,
    e.latitud,
    e.longitud,
    e.tiene_email,
    e.tiene_web
FROM denue_clean e
LEFT JOIN dim_geografia g
    ON TRIM(e.entidad)   = g.entidad
    AND TRIM(e.municipio) = g.municipio
    AND TRIM(e.localidad) = g.localidad
LEFT JOIN dim_tamanio t
    ON e.tamanio_empresa = t.tamanio_nombre;
"""

if __name__ == "__main__":
    print("🏗️  Construyendo star schema...")

    with engine.connect() as conn:
        for statement in SCHEMA_QUERY.strip().split(";"):
            statement = statement.strip()
            if statement:
                print(f"   ⚙️  Ejecutando: {statement[:50]}...")
                conn.execute(text(statement))
                conn.commit()

    with engine.connect() as conn:
        tablas = ["dim_sector", "dim_geografia", "dim_tamanio", "dim_tiempo", "fact_empresas"]
        print("\n✅ Schema creado:")
        for tabla in tablas:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}"))
            count = result.scalar()
            print(f"   {tabla:<20} : {count:,} registros")