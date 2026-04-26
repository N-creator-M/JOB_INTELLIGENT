from fastapi import FastAPI
import os
from sqlalchemy import create_engine, text

app = FastAPI(title="Job Intelligent API")

def get_gold_db():
    url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','ji_user')}:{os.getenv('POSTGRES_PASSWORD','ji_123')}@{os.getenv('POSTGRES_HOST','postgres')}:5432/{os.getenv('POSTGRES_GOLD_DB','gold_db')}"
    return create_engine(url)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/kpis")
def get_kpis():
    engine = get_gold_db()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT titre_normalise, nb_offres, salaire_moy, top_localisations
            FROM gold.kpi_par_metier
            ORDER BY nb_offres DESC
        """))
        rows = result.fetchall()
    return [
        {
            "titre": row[0],
            "nb_offres": row[1],
            "salaire_moy": float(row[2]) if row[2] else None,
            "top_localisations": row[3]
        }
        for row in rows
    ]

@app.get("/stats")
def get_stats():
    engine = get_gold_db()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT total_offres, offres_cdi, offres_cdd, offres_remote
            FROM gold.stats_marche
            ORDER BY date_calcul DESC
            LIMIT 1
        """))
        row = result.fetchone()
    if not row:
        return {"message": "Aucune donnée disponible"}
    return {
        "total_offres": row[0],
        "offres_cdi": row[1],
        "offres_cdd": row[2],
        "offres_remote": row[3]
    }

@app.get("/offres")
def get_offres():
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','ji_user')}:{os.getenv('POSTGRES_PASSWORD','ji_123')}@{os.getenv('POSTGRES_HOST','postgres')}:5432/{os.getenv('POSTGRES_SILVER_DB','silver_db')}"
    )
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT source, titre, entreprise, localisation, contrat, competences
            FROM silver.offres
            ORDER BY created_at DESC
        """))
        rows = result.fetchall()
    return [
        {
            "source": row[0],
            "titre": row[1],
            "entreprise": row[2],
            "localisation": row[3],
            "contrat": row[4],
            "competences": row[5]
        }
        for row in rows
    ]