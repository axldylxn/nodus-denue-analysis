import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm

#-config
#load var .env 

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

#create the conection to PostgreSQL using SQLAlchemy

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
DATA_PATH = "data/raw/"
CHUNK_SIZE = 50_000 #process 50,000 raws to avoid saturate RAM

#take DENUE's relevant colums
COLS_TO_KEEP = ["id", "nom_estab", "raz_social", "codigo_act", "nombre_act",
    "per_ocu", "tipounieco", "latitud", "longitud",
    "entidad", "municipio", "localidad", "nombre_asen",
    "correoelec", "www", "fecha_alta"]

#Ingest principal function

def ingest_csv (filepath: str, table_name: str = "denue_raw"):
    print (f"\n📂 Procesando: {filepath}")
          
    #read the csv into chunks

    chunk_iter = pd.read_csv(
        filepath,
        encoding="latin-1", #INEGI uses latin-1, NO utf-8
        chunksize = CHUNK_SIZE,
        low_memory=False,
        usecols=lambda c: c in COLS_TO_KEEP
    )

    total_rows = 0

    for i, chunk in enumerate(tqdm(chunk_iter, desc=" Cargando")):
        #Normalizes colums names
        chunk.columns = [c.lower().strip() for c in chunk.columns]
        #Insert the chunk in PostgreSQL
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        total_rows += len(chunk)

    print(f" ✅ {total_rows:,} filas cargadas desde {os.path.basename(filepath)}")
    return total_rows

#execution
if __name__ == "__main__":
    #Look for all the csv into the data/raw/
    csv_files = glob.glob(os.path.join(DATA_PATH, "**/*.csv"), recursive=True)
    csv_files += glob.glob(os.path.join(DATA_PATH, "*.csv"))
    csv_files = list(set(csv_files)) #eliminate duplicate
    csv_files = [f for f in csv_files if "diccionario" not in f.lower()]

    if not csv_files:
        print("❌ No se encontraron CSVs en data/raw/")
        exit(1)

    print(f"🗂️  Archivos CSV encontrados: {len(csv_files)}")

    grand_total = 0
    for f in csv_files:
        grand_total += ingest_csv(f)
        
    print(f"\n🎉 Ingesta completa: {grand_total:,} filas totales")

    #Final verification in PostgreSQL

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM denue_raw"))
        count = result.scalar()
        print(f"📊 Confirmado en PostgreSQL: {count:,} registros")