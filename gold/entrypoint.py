"""
Gold — Entrypoint
Pipeline : Silver (MinIO Parquet) → Gold (PostgreSQL Star Schema uniquement)

Étapes :
  1. Lecture Silver depuis MinIO
  2. Calcul KPIs marché
  3. Chargement Star Schema dans PostgreSQL gold_db (schéma gold)

Usage :
  python entrypoint.py
  python entrypoint.py --date 2026-04-29
  python entrypoint.py --skip-postgres
"""
import argparse
from dotenv import load_dotenv
from loguru import logger

from minio_handler import MinIOHandler
from aggregations.kpis import KPICalculator

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="Gold aggregation pipeline")
    parser.add_argument("--date", default=None, help="Date à traiter (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Ignore le chargement PostgreSQL",
    )
    args = parser.parse_args()

    logger.info("══ Démarrage pipeline Gold ══")

    # ── 1. Lecture Silver ──────────────────────────────
    handler = MinIOHandler()
    df      = handler.read_silver(target_date=args.date)

    if df.empty:
        logger.error("Aucune donnée Silver trouvée. Lance d'abord le pipeline Silver.")
        return

    logger.info(f"Silver chargé : {len(df)} offres")

    # ── 2. Calcul KPIs ─────────────────────────────────
    calc = KPICalculator()
    kpi_metier  = calc.compute_kpi_par_metier(df)
    kpi_source  = calc.compute_kpi_par_source(df)
    tendances   = calc.compute_tendances_competences(df)
    stats       = calc.compute_stats_marche(df)

    # ── 3. Chargement PostgreSQL Star Schema ───────────
    if not args.skip_postgres:
        try:
            from postgres_loader import PostgresLoader
            loader = PostgresLoader()

            map_metier  = loader.load_dim_metier(df)
            map_loc     = loader.load_dim_localisation(df)
            map_date    = loader.load_dim_date(df)
            map_source  = loader.load_sources_mapping()
            map_contrat = loader.load_contrats_mapping()

            loader.load_fait_offres(
                df, map_metier, map_loc,
                map_source, map_contrat, map_date
            )
            loader.close()
            logger.success("[PostgreSQL] Star Schema chargé avec succès.")

        except Exception as e:
            logger.error(f"Erreur PostgreSQL : {e}")
            logger.warning("Lance avec --skip-postgres pour ignorer PostgreSQL.")
    else:
        logger.info("PostgreSQL ignoré (--skip-postgres)")

    logger.success(f"══ Gold terminé — {len(df)} offres traitées ══")


if __name__ == "__main__":
    main()