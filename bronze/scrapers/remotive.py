"""
Scraper Remotive
API publique : https://remotive.com/api/remote-jobs
Gratuit      : Oui — aucune inscription, aucune clé API
Spécialité   : Offres remote tech, data, AI
"""
import time
import requests
from datetime import datetime
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://remotive.com/api/remote-jobs"

# Catégories data/tech disponibles sur Remotive
CATEGORIES = [
    "data",
    "software-dev",
    "machine-learning",
]

# Mots-clés à filtrer dans les titres
DATA_KEYWORDS = [
    "data engineer",
    "data scientist",
    "data analyst",
    "machine learning",
    "mlops",
    "data architect",
    "business intelligence",
    "big data",
    "nlp",
    "ai engineer",
    "analytics",
]


class RemotiveScraper:

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20))
    def _fetch(self, category: str) -> list[dict]:
        """Récupère les offres d'une catégorie Remotive."""
        resp = requests.get(
            BASE_URL,
            params={"category": category, "limit": 100},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("jobs", [])

    @staticmethod
    def _is_data_job(title: str) -> bool:
        """Filtre les offres pertinentes pour le domaine data."""
        title_lower = title.lower()
        return any(kw in title_lower for kw in DATA_KEYWORDS)

    @staticmethod
    def _normalize(raw: dict) -> dict:
        """Convertit une offre Remotive en format unifié."""
        return {
            "source":        "remotive",
            "source_id":     str(raw.get("id", "")),
            "keyword":       "remote_data",
            "titre":         raw.get("title", ""),
            "entreprise":    raw.get("company_name", ""),
            "localisation":  raw.get("candidate_required_location", "Worldwide"),
            "contrat":       raw.get("job_type", ""),
            "telecommande":  True,
            "description":   raw.get("description", ""),
            "salaire_min":   None,
            "salaire_max":   None,
            "categorie":     raw.get("category", ""),
            "tags":          raw.get("tags", []),
            "url":           raw.get("url", ""),
            "date_creation": raw.get("publication_date", ""),
            "scraped_at":    datetime.utcnow().isoformat(),
        }

    def scrape(self) -> list[dict]:
        """Lance le scraping Remotive pour toutes les catégories data."""
        all_jobs = []
        seen_ids = set()

        for category in CATEGORIES:
            logger.info(f"[Remotive] Catégorie : '{category}'")
            try:
                jobs = self._fetch(category)
                for job in jobs:
                    sid = str(job.get("id", ""))
                    # Filtre sur les titres pertinents data
                    if sid not in seen_ids and self._is_data_job(job.get("title", "")):
                        seen_ids.add(sid)
                        all_jobs.append(self._normalize(job))

                logger.info(f"  → {len(jobs)} offres récupérées, filtrées pour data")
                time.sleep(1)

            except Exception as e:
                logger.error(f"[Remotive] Erreur catégorie '{category}': {e}")
                continue

        logger.success(f"[Remotive] Total : {len(all_jobs)} offres data uniques.")
        return all_jobs