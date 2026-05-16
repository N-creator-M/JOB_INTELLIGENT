"""
Silver — Entrypoint
Pipeline complet : Bronze (MinIO JSON) → Silver (MinIO Parquet)
Étapes :
  1. Lecture des offres brutes depuis Bronze (MinIO)
  2. Nettoyage et déduplication
  3. Normalisation des titres et salaires
  4. Extraction des compétences NLP
  5. Génération des embeddings (optionnel)
  6. Écriture en Parquet dans Silver (MinIO)

Usage :
  python entrypoint.py
  python entrypoint.py --date 2026-04-29
  python entrypoint.py --skip-embeddings
"""
import argparse
from dotenv import load_dotenv
from loguru import logger

from minio_handler import MinIOHandler
from transforms.cleaner import Cleaner
from transforms.normalizer import Normalizer
from nlp.skills_extractor import SkillsExtractor

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="Silver transformation pipeline")
    parser.add_argument("--date", default=None, help="Date à traiter (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Ignore la génération des embeddings (NLP service non requis)",
    )
    args = parser.parse_args()

    logger.info("══ Démarrage pipeline Silver ══")

    # ── 1. Lecture Bronze ──────────────────────────────
    handler = MinIOHandler()
    jobs    = handler.read_bronze(target_date=args.date)

    if not jobs:
        logger.error("Aucune offre trouvée dans Bronze. Vérifie que le pipeline Bronze a tourné.")
        return

    # ── 2. Nettoyage & déduplication ──────────────────
    cleaner = Cleaner()
    df      = cleaner.clean(jobs)

    if df.empty:
        logger.error("DataFrame vide après nettoyage.")
        return

    # ── 3. Normalisation ───────────────────────────────
    normalizer = Normalizer()
    df         = normalizer.normalize(df)

    # ── 4. Extraction des compétences ─────────────────
    extractor = SkillsExtractor()
    df        = extractor.extract(df)

    # ── 5. Embeddings (optionnel) ─────────────────────
    if not args.skip_embeddings:
        try:
            from nlp.embedder import Embedder
            embedder = Embedder()
            df       = embedder.embed(df)
        except Exception as e:
            logger.warning(f"Embeddings ignorés : {e}")
            df["embedding"] = None
    else:
        logger.info("Embeddings ignorés (--skip-embeddings)")
        df["embedding"] = None

    # ── 6. Écriture Silver (MinIO Parquet) ─────────────
    path = handler.write_silver(df)

    logger.success(f"══ Silver terminé — {len(df)} offres → {path} ══")


if __name__ == "__main__":
    main()
