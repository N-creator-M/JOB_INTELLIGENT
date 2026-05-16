"""
Silver — Skills Extractor
Extraction des compétences techniques depuis les descriptions
Approche : matching par dictionnaire (pas besoin de spaCy pour démarrer)
"""
import re
import pandas as pd
from loguru import logger


# Dictionnaire des compétences data/tech
SKILLS_DICT = {
    # Anglais — langages
"python", "sql", "scala", "java", "go",
# Anglais — cloud
"aws", "azure", "gcp", "s3", "redshift", "bigquery", "snowflake",
# Anglais — ML
"tensorflow", "pytorch", "scikit-learn", "xgboost", "lightgbm",
"mlflow", "hugging face", "transformers", "llm", "rag",
# Anglais — data engineering  
"spark", "kafka", "airflow", "dbt", "databricks",
"data lake", "data warehouse", "parquet", "delta lake",
# Anglais — DevOps
"docker", "kubernetes", "git", "ci/cd", "terraform",
}


class SkillsExtractor:

    def _extract_skills(self, text: str) -> list[str]:
        """Extrait les compétences d'un texte par matching dictionnaire."""
        if not text or not isinstance(text, str):
            return []

        text_lower = text.lower()
        found = set()

        for skill in SKILLS_DICT:
            # Recherche avec word boundary pour éviter les faux positifs
            pattern = r"\b" + re.escape(skill) + r"\b"
            if re.search(pattern, text_lower):
                found.add(skill)

        return sorted(list(found))

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ajoute une colonne 'competences' avec les skills extraits."""
        if df.empty:
            return df

        logger.info(f"[SkillsExtractor] Extraction des compétences sur {len(df)} offres...")

        # Combine titre + description pour l'extraction
        df["competences"] = df.apply(
            lambda r: self._extract_skills(
                f"{r.get('titre', '')} {r.get('description', '')}"
            ),
            axis=1,
        )

        # Stats
        total_skills = df["competences"].apply(len).sum()
        avg_skills   = df["competences"].apply(len).mean()
        logger.success(
            f"[SkillsExtractor] {total_skills} compétences extraites "
            f"(moy. {avg_skills:.1f} par offre)"
        )

        return df
