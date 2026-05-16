"""
Scraper France Travail
API officielle : https://francetravail.io/data/api/offres-emploi
Auth          : OAuth2 (client_credentials)
Endpoint      : GET /partenaire/offresdemploi/v2/offres/search
Gratuit       : Oui — nécessite inscription sur francetravail.io
"""
import os
import time
import requests
from datetime import datetime
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

# Mots-clés data — couvre les principaux métiers du domaine
DATA_KEYWORDS = [
    "data engineer",
    "data scientist",
    "data analyst",
    "machine learning engineer",
    "MLOps",
    "data architect",
    "business intelligence",
    "big data",
    "NLP engineer",
    "AI engineer",
]


class FranceTravailScraper:

    def __init__(self):
        self.client_id     = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
        self.client_secret = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")
        self.token         = None
        self.token_expiry  = 0

    # ── Auth ──────────────────────────────────────────

    def _get_token(self) -> str:
        """Récupère ou renouvelle le token OAuth2."""
        if self.token and time.time() < self.token_expiry - 60:
            return self.token

        logger.info("[FranceTravail] Récupération du token OAuth2...")
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
                "scope":         "api_offresdemploiv2 o2dsoffre",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token        = data["access_token"]
        self.token_expiry = time.time() + data.get("expires_in", 1500)
        logger.success("[FranceTravail] Token obtenu.")
        return self.token

    # ── Recherche ─────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20))
    def _search(self, keyword: str, start: int = 0) -> dict:
        """Appel paginé à l'endpoint de recherche."""
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept":        "application/json",
        }
        params = {
            "motsCles":        keyword,
            "typeContrat":     "CDI,CDD,MIS,SAI",
            "range":           f"{start}-{start + 49}",   # 50 résultats par page
            "sort":            "1",                        # tri par date
        }
        resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30)

        # 206 = résultats partiels (pagination), 200 = tous les résultats
        if resp.status_code not in (200, 206):
            logger.warning(f"[FranceTravail] HTTP {resp.status_code} pour '{keyword}'")
            resp.raise_for_status()

        return resp.json()

    # ── Normalisation ─────────────────────────────────

    @staticmethod
    def _normalize(raw: dict, keyword: str) -> dict:
        """Convertit une offre brute France Travail en format unifié."""
        lieu = raw.get("lieuTravail", {})
        sal  = raw.get("salaire", {})
        ent  = raw.get("entreprise", {})

        return {
            "source":            "france_travail",
            "source_id":         raw.get("id"),
            "keyword":           keyword,
            "titre":             raw.get("intitule", ""),
            "entreprise":        ent.get("nom", ""),
            "localisation":      lieu.get("libelle", ""),
            "code_postal":       lieu.get("codePostal", ""),
            "contrat":           raw.get("typeContrat", ""),
            "contrat_libelle":   raw.get("typeContratLibelle", ""),
            "experience":        raw.get("experienceLibelle", ""),
            "formation":         raw.get("niveauExperienceLibelle", ""),
            "description":       raw.get("description", ""),
            "salaire_libelle":   sal.get("libelle", ""),
            "salaire_min":       sal.get("min"),
            "salaire_max":       sal.get("max"),
            "date_creation":     raw.get("dateCreation"),
            "date_actualisation":raw.get("dateActualisation"),
            "url":               raw.get("origineOffre", {}).get("urlOrigine", ""),
            "telecommande":      raw.get("deplacementCode", "") == "O",
            "scraped_at":        datetime.utcnow().isoformat(),
        }

    # ── Point d'entrée ────────────────────────────────

    def scrape(self) -> list[dict]:
        """Lance le scraping pour tous les mots-clés data."""
        all_jobs = []
        seen_ids = set()

        for keyword in DATA_KEYWORDS:
            logger.info(f"[FranceTravail] Recherche : '{keyword}'")
            start = 0

            while True:
                try:
                    data      = self._search(keyword, start)
                    offres    = data.get("resultats", [])
                    total     = data.get("Content-Range", {})

                    if not offres:
                        break

                    for offre in offres:
                        sid = offre.get("id")
                        if sid and sid not in seen_ids:
                            seen_ids.add(sid)
                            all_jobs.append(self._normalize(offre, keyword))

                    logger.info(f"  → {len(offres)} offres récupérées (start={start})")

                    # Pagination : 50 résultats par page, max 150 par keyword
                    start += 50
                    if start >= 150 or len(offres) < 50:
                        break

                    time.sleep(1)   # respecte le rate limit

                except Exception as e:
                    logger.error(f"[FranceTravail] Erreur pour '{keyword}': {e}")
                    break

        logger.success(f"[FranceTravail] Total : {len(all_jobs)} offres uniques.")
        return all_jobs
