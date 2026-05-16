"""
Silver — Cleaner
Nettoyage et déduplication des offres brutes
"""
import hashlib
import re
import pandas as pd
from loguru import logger


class Cleaner:

    # ── Nettoyage texte ───────────────────────────────

    @staticmethod
    def _clean_text(text: str) -> str:
        """Nettoie un texte brut — supprime HTML, espaces multiples."""
        if not text or not isinstance(text, str):
            return ""
        # Supprime les balises HTML
        text = re.sub(r"<[^>]+>", " ", text)
        # Supprime les caractères spéciaux sauf ponctuation utile
        text = re.sub(r"[^\w\s\.,;:!?éèêëàâùûüôîïç\-]", " ", text)
        # Normalise les espaces
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _clean_titre(titre: str) -> str:
        """Nettoie un titre de poste."""
        if not titre or not isinstance(titre, str):
            return ""
        titre = re.sub(r"\s*\(H/F\)|\(F/H\)|\(M/F\)", "", titre, flags=re.IGNORECASE)
        titre = re.sub(r"\s+", " ", titre).strip()
        return titre

    @staticmethod
    def _clean_salaire(val) -> float | None:
        """Convertit une valeur de salaire en float."""
        if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(str(val).replace(",", ".").replace(" ", ""))
        except (ValueError, TypeError):
            return None

    # ── Déduplication ─────────────────────────────────

    @staticmethod
    def _compute_hash(row: dict) -> str:
        """Calcule un hash SHA-256 pour détecter les doublons."""
        key = f"{row.get('titre', '')}|{row.get('entreprise', '')}|{row.get('localisation', '')}"
        return hashlib.sha256(key.lower().encode()).hexdigest()

    # ── Pipeline nettoyage ────────────────────────────

    def clean(self, jobs: list[dict]) -> pd.DataFrame:
        """
        Nettoie une liste d'offres brutes.
        Retourne un DataFrame dédupliqué et nettoyé.
        """
        if not jobs:
            logger.warning("[Cleaner] Aucune offre à nettoyer.")
            return pd.DataFrame()

        df = pd.DataFrame(jobs)
        initial_count = len(df)
        logger.info(f"[Cleaner] {initial_count} offres en entrée")

        # Nettoyage des champs texte
        df["titre"]       = df["titre"].apply(self._clean_titre)
        df["description"] = df["description"].apply(self._clean_text)
        df["entreprise"]  = df["entreprise"].fillna("").str.strip()
        df["localisation"]= df["localisation"].fillna("").str.strip()
        df["contrat"]     = df["contrat"].fillna("").str.upper().str.strip()

        # Nettoyage des salaires
        df["salaire_min"] = df["salaire_min"].apply(self._clean_salaire)
        df["salaire_max"] = df["salaire_max"].apply(self._clean_salaire)

        # Suppression des offres sans titre ni description
        df = df[df["titre"].str.len() > 2]
        df = df[df["description"].str.len() > 10]

        # Calcul du hash pour déduplication
        df["hash_dedup"] = df.apply(self._compute_hash, axis=1)

        # Déduplication
        df = df.drop_duplicates(subset=["hash_dedup"], keep="first")

        final_count = len(df)
        logger.success(
            f"[Cleaner] {final_count} offres après nettoyage "
            f"({initial_count - final_count} doublons supprimés)"
        )

        return df.reset_index(drop=True)
