"""
Scraper Adzuna
API officielle : https://developer.adzuna.com
Inscription    : https://developer.adzuna.com/signup (gratuit)
Limite         : 100 requêtes / jour (plan gratuit)
Endpoint       : GET /api/v1/jobs/{country}/search/{page}
Pays couverts  : fr (France), be (Belgique), ch (Suisse)
"""
import os
import time
import requests
from datetime import datetime
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


BASE_URL = "https://api.adzuna.com/v1/api/jobs"

DATA_KEYWORDS = [
    "data engineer",
    "data scientist",
    "data analyst",
    "machine learning",
    "MLOps",
    "data architect",
    "business intelligence",
    "NLP",
    "AI engineer",
    "big data",
]

# Pays à scraper (couverts par Adzuna)
COUNTRIES = ["fr", "be", "ch"]


class AdzunaScraper:

    def __init__(self):
        self.app_id  = os.getenv("ADZUNA_APP_ID")
        self.api_key = os.getenv("ADZUNA_API_KEY")

        if not self.app_id or not self.api_key:
            raise ValueError(
                "ADZUNA_APP_ID et ADZUNA_API_KEY manquants dans .env\n"
                "Inscription gratuite : https://developer.adzuna.com/signup"
            )

    # ── Appel API ─────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20))
    def _search(self, keyword: str, country: str, page: int = 1) -> dict:
        """Appel paginé à l'API Adzuna."""
        url = f"{BASE_URL}/{country}/search/{page}"
        params = {
            "app_id":          self.app_id,
            "app_key":         self.api_key,
            "what":            keyword,
            "results_per_page":50,
            "sort_by":         "date",
            "content-type":    "application/json",
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ── Normalisation ─────────────────────────────────

    @staticmethod
    def _normalize(raw: dict, keyword: str, country: str) -> dict:
        """Convertit une offre Adzuna en format unifié."""
        loc = raw.get("location", {})
        cat = raw.get("category", {})
        sal = raw

        return {
            "source":          "adzuna",
            "source_id":       raw.get("id"),
            "keyword":         keyword,
            "pays":            country,
            "titre":           raw.get("title", ""),
            "entreprise":      raw.get("company", {}).get("display_name", ""),
            "localisation":    loc.get("display_name", ""),
            "area":            loc.get("area", []),
            "contrat":         raw.get("contract_type", ""),
            "contrat_time":    raw.get("contract_time", ""),
            "description":     raw.get("description", ""),
            "salaire_min":     sal.get("salary_min"),
            "salaire_max":     sal.get("salary_max"),
            "salaire_is_pred": sal.get("salary_is_predicted", False),
            "categorie":       cat.get("label", ""),
            "url":             raw.get("redirect_url", ""),
            "date_creation":   raw.get("created"),
            "scraped_at":      datetime.utcnow().isoformat(),
        }

    # ── Point d'entrée ────────────────────────────────

    def scrape(self) -> list[dict]:
        """Lance le scraping pour tous les mots-clés et pays."""
        all_jobs = []
        seen_ids = set()
        request_count = 0
        MAX_REQUESTS = 90   # marge de sécurité sous la limite de 100/jour

        for country in COUNTRIES:
            for keyword in DATA_KEYWORDS:

                if request_count >= MAX_REQUESTS:
                    logger.warning("[Adzuna] Limite journalière atteinte, arrêt.")
                    return all_jobs

                logger.info(f"[Adzuna] '{keyword}' — pays: {country}")

                for page in range(1, 4):   # max 3 pages = 150 résultats par keyword/pays
                    if request_count >= MAX_REQUESTS:
                        break
                    try:
                        data   = self._search(keyword, country, page)
                        offres = data.get("results", [])
                        request_count += 1

                        if not offres:
                            break

                        for offre in offres:
                            sid = offre.get("id")
                            if sid and sid not in seen_ids:
                                seen_ids.add(sid)
                                all_jobs.append(self._normalize(offre, keyword, country))

                        logger.info(f"  → page {page}: {len(offres)} offres")

                        if len(offres) < 50:
                            break

                        time.sleep(1.5)   # respecte le rate limit Adzuna

                    except Exception as e:
                        logger.error(f"[Adzuna] Erreur '{keyword}' / {country} page {page}: {e}")
                        break

        logger.success(f"[Adzuna] Total : {len(all_jobs)} offres uniques ({request_count} requêtes).")
        return all_jobs
