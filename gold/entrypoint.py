import argparse
import os
from sqlalchemy import create_engine, text

def get_silver_db():
    url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','ji_user')}:{os.getenv('POSTGRES_PASSWORD','ji_123')}@{os.getenv('POSTGRES_HOST','postgres')}:5432/{os.getenv('POSTGRES_SILVER_DB','silver_db')}"
    return create_engine(url)

def get_gold_db():
    url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','ji_user')}:{os.getenv('POSTGRES_PASSWORD','ji_123')}@{os.getenv('POSTGRES_HOST','postgres')}:5432/{os.getenv('POSTGRES_GOLD_DB','gold_db')}"
    return create_engine(url)

def compute_kpis():
    silver = get_silver_db()
    gold = get_gold_db()
    with silver.connect() as s_conn:
        result = s_conn.execute(text("""
            SELECT titre_normalise, COUNT(*) as nb_offres,
                AVG(salaire_min) as salaire_moy,
                MIN(salaire_min) as salaire_min,
                MAX(salaire_max) as salaire_max,
                ARRAY_AGG(DISTINCT localisation) FILTER (WHERE localisation IS NOT NULL) as top_localisations
            FROM silver.offres
            GROUP BY titre_normalise
        """))
        rows = result.fetchall()
    with gold.begin() as g_conn:
        for row in rows:
            g_conn.execute(text("""
                INSERT INTO gold.kpi_par_metier
                    (titre_normalise, nb_offres, salaire_moy, salaire_min, salaire_max, top_localisations, date_calcul)
                VALUES
                    (:titre, :nb, :moy, :min, :max, :locs, CURRENT_DATE)
            """), {
                "titre": row[0],
                "nb": row[1],
                "moy": row[2],
                "min": row[3],
                "max": row[4],
                "locs": list(row[5]) if row[5] else []
            })
    print(f"[kpis] {len(rows)} KPIs calculés")

def compute_stats():
    silver = get_silver_db()
    gold = get_gold_db()
    with silver.connect() as s_conn:
        result = s_conn.execute(text("""
            SELECT COUNT(*) as total,
                COUNT(*) FILTER (WHERE contrat ILIKE '%CDI%') as cdi,
                COUNT(*) FILTER (WHERE contrat ILIKE '%CDD%') as cdd,
                COUNT(*) FILTER (WHERE description ILIKE '%remote%' OR description ILIKE '%télétravail%') as remote
            FROM silver.offres
        """))
        stats = result.fetchone()
    with gold.begin() as g_conn:
        g_conn.execute(text("""
            INSERT INTO gold.stats_marche
                (total_offres, offres_cdi, offres_cdd, offres_remote, date_calcul)
            VALUES
                (:total, :cdi, :cdd, :remote, CURRENT_DATE)
        """), {
            "total": stats[0],
            "cdi": stats[1],
            "cdd": stats[2],
            "remote": stats[3]
        })
    print(f"[stats] Stats marché calculées — {stats[0]} offres au total")
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["kpis", "stats", "all"], default="all")
    args = parser.parse_args()

    if args.step in ("kpis", "all"):
        compute_kpis()

    if args.step in ("stats", "all"):
        compute_stats()