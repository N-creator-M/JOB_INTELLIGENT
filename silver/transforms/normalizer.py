"""
Silver — Normalizer
Standardisation des titres de postes et salaires
"""
import re
import pandas as pd
from loguru import logger


# Mapping des titres vers une forme canonique
TITRE_MAPPING = {
    # Data Engineer
    r"data eng(ineer|ineer)?": "Data Engineer",
    r"ingénieur(e)? data": "Data Engineer",
    r"engineer data": "Data Engineer",
    # Data Scientist
    r"data sci(entist)?": "Data Scientist",
    r"scientifique des données": "Data Scientist",
    # Data Analyst
    r"data anal(yst|yste)?": "Data Analyst",
    r"analyste (de )?données": "Data Analyst",
    r"analyste data": "Data Analyst",
    # Machine Learning
    r"machine learning eng(ineer)?": "ML Engineer",
    r"ml eng(ineer)?": "ML Engineer",
    r"ingénieur(e)? machine learning": "ML Engineer",
    # MLOps
    r"mlops eng(ineer)?": "MLOps Engineer",
    r"ml ?ops": "MLOps Engineer",
    # Business Intelligence
    r"bi (dev(eloper)?|eng(ineer)?|analyst)": "BI Developer",
    r"business intel(ligence)?": "BI Developer",
    r"développeur(se)? bi": "BI Developer",
    # Data Architect
    r"data arch(itect)?": "Data Architect",
    r"architecte data": "Data Architect",
    # NLP / AI
    r"nlp eng(ineer)?": "NLP Engineer",
    r"ai eng(ineer)?": "AI Engineer",
    r"ingénieur(e)? ia": "AI Engineer",
    # Analytics
    r"analytics eng(ineer)?": "Analytics Engineer",
    r"data platform eng(ineer)?": "Data Platform Engineer",
}

# Mapping des types de contrat
CONTRAT_MAPPING = {
    "CDI": "CDI",
    "CDD": "CDD",
    "FULL_TIME": "CDI",
    "PART_TIME": "Temps partiel",
    "CONTRACT": "Freelance",
    "FREELANCE": "Freelance",
    "INTERNSHIP": "Stage",
    "STAGE": "Stage",
    "ALTERNANCE": "Alternance",
    "MIS": "Intérim",
    "SAI": "Saisonnier",
}


class Normalizer:

    # ── Titres ────────────────────────────────────────

    @staticmethod
    def _normalize_titre(titre: str) -> str:
        """Normalise un titre de poste vers une forme canonique."""
        if not titre:
            return "Autre"
        titre_lower = titre.lower()
        for pattern, canonical in TITRE_MAPPING.items():
            if re.search(pattern, titre_lower):
                return canonical
        return titre.title()

    # ── Contrats ──────────────────────────────────────

    @staticmethod
    def _normalize_contrat(contrat: str) -> str:
        """Normalise un type de contrat."""
        if not contrat:
            return "Non précisé"
        contrat_upper = contrat.upper().strip()
        return CONTRAT_MAPPING.get(contrat_upper, contrat.title())

    # ── Salaires ──────────────────────────────────────

    @staticmethod
    def _normalize_salaire(val: float | None, source: str) -> float | None:
        """
        Normalise un salaire en euros annuels bruts.
        Adzuna retourne parfois des salaires mensuels ou en devises.
        """
        if val is None or pd.isna(val):
            return None
        # Si le salaire semble mensuel (< 5000), le convertir en annuel
        if val < 5000:
            return round(val * 12, 2)
        return round(val, 2)

    # ── Pipeline normalisation ────────────────────────

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalise les titres, contrats et salaires."""
        if df.empty:
            return df

        logger.info(f"[Normalizer] Normalisation de {len(df)} offres...")

        df["titre_normalise"] = df["titre"].apply(self._normalize_titre)
        df["contrat"]         = df["contrat"].apply(self._normalize_contrat)

        df["salaire_min"] = df.apply(
            lambda r: self._normalize_salaire(r.get("salaire_min"), r.get("source", "")),
            axis=1,
        )
        df["salaire_max"] = df.apply(
            lambda r: self._normalize_salaire(r.get("salaire_max"), r.get("source", "")),
            axis=1,
        )

        # Détection télétravail depuis la description
        remote_pattern = r"télétravail|remote|full.remote|hybrid|distanciel"
        df["telecommande"] = df.get("telecommande", False) | df["description"].str.contains(
            remote_pattern, case=False, na=False
        )

        logger.success(f"[Normalizer] Normalisation terminée.")
        return df
