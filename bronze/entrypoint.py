"""
Entrypoint Bronze
Appelé par le DockerOperator Airflow :
  python entrypoint.py --source france_travail
  python entrypoint.py --source adzuna
  python entrypoint.py --source jobspy
  python entrypoint.py --source all
"""
import argparse
import sys
from dotenv import load_dotenv
from loguru import logger
from loaders.minio_writer import MinIOWriter

load_dotenv()


def run_source(source: str, writer: MinIOWriter) -> int:
    """Lance un scraper et écrit le résultat dans MinIO.
    Les imports sont lazy — chaque scraper n'est importé que si nécessaire.
    """
    logger.info(f"══ Démarrage scraper : {source} ══")

    if source == "france_travail":
        from scrapers.france_travail import FranceTravailScraper
        jobs = FranceTravailScraper().scrape()

    elif source == "adzuna":
        from scrapers.adzuna import AdzunaScraper
        jobs = AdzunaScraper().scrape()

    elif source == "remotive":
        from scrapers.remotive import RemotiveScraper
        jobs = RemotiveScraper().scrape()

    else:
        logger.error(f"Source inconnue : {source}")
        return 0

    if jobs:
        writer.write(jobs, source)
    else:
        logger.warning(f"Aucune offre récupérée pour '{source}'")

    return len(jobs)


def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion pipeline")
    parser.add_argument(
        "--source",
        choices=["france_travail", "adzuna", "remotive", "all"],
        default="all",
        help="Source à scraper (défaut : all)",
    )
    args = parser.parse_args()

    writer = MinIOWriter()
    total  = 0

    if args.source == "all":
        for src in ["france_travail", "adzuna", "remotive"]:
            try:
                total += run_source(src, writer)
            except Exception as e:
                logger.error(f"Scraper '{src}' échoué : {e}")
    else:
        try:
            total = run_source(args.source, writer)
        except Exception as e:
            logger.error(f"Scraper '{args.source}' échoué : {e}")
            sys.exit(1)

    logger.success(f"══ Bronze terminé — {total} offres ingérées au total ══")


if __name__ == "__main__":
    main()