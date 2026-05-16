"""
Gold — Cosine Matcher
Système de recommandation basé sur la similarité cosinus
entre le profil candidat et les embeddings des offres
"""
import json
import numpy as np
import pandas as pd
from loguru import logger


class CosineMatcher:

    @staticmethod
    def _cosine_similarity(vec_a: list, vec_b: list) -> float:
        """Calcule la similarité cosinus entre deux vecteurs."""
        a = np.array(vec_a, dtype=float)
        b = np.array(vec_b, dtype=float)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    def match_by_skills(
        self,
        df: pd.DataFrame,
        candidate_skills: list[str],
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Recommande des offres basées sur le matching de compétences.
        Utilisé quand les embeddings ne sont pas disponibles.
        """
        if df.empty or not candidate_skills:
            return pd.DataFrame()

        candidate_set = set(s.lower() for s in candidate_skills)

        def skill_score(offre_skills):
            if not isinstance(offre_skills, list):
                return 0.0
            offre_set = set(s.lower() for s in offre_skills)
            if not offre_set:
                return 0.0
            intersection = candidate_set & offre_set
            union = candidate_set | offre_set
            return len(intersection) / len(union)  # Jaccard similarity

        df = df.copy()
        df["score"] = df["competences"].apply(skill_score)
        df = df[df["score"] > 0].sort_values("score", ascending=False).head(top_n)
        df["rang"] = range(1, len(df) + 1)

        logger.info(f"[Recommender] {len(df)} offres recommandées par skills")
        return df[["titre", "entreprise", "localisation", "contrat",
                   "salaire_min", "salaire_max", "competences",
                   "url", "score", "rang"]]

    def match_by_embedding(
        self,
        df: pd.DataFrame,
        candidate_embedding: list[float],
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Recommande des offres basées sur la similarité cosinus
        entre l'embedding du profil et les embeddings des offres.
        """
        if df.empty or not candidate_embedding:
            return pd.DataFrame()

        # Filtre les offres avec embeddings valides
        df_valid = df[df["embedding"].apply(
            lambda x: isinstance(x, list) and len(x) > 0
        )].copy()

        if df_valid.empty:
            logger.warning("[Recommender] Aucune offre avec embedding — fallback skills")
            return pd.DataFrame()

        df_valid["score"] = df_valid["embedding"].apply(
            lambda emb: self._cosine_similarity(candidate_embedding, emb)
        )

        df_valid = df_valid.sort_values("score", ascending=False).head(top_n)
        df_valid["rang"] = range(1, len(df_valid) + 1)

        logger.info(f"[Recommender] {len(df_valid)} offres recommandées par embedding")
        return df_valid[["titre", "entreprise", "localisation", "contrat",
                         "salaire_min", "salaire_max", "competences",
                         "url", "score", "rang"]]

    def build_recommendations_table(
        self,
        df: pd.DataFrame,
        profiles: list[dict],
    ) -> pd.DataFrame:
        """
        Construit la table de recommandations pour un ensemble de profils.
        Chaque profil = {"session_id": str, "skills": list, "embedding": list}
        """
        all_recs = []

        for profile in profiles:
            session_id = profile.get("session_id", "unknown")
            skills     = profile.get("skills", [])
            embedding  = profile.get("embedding")

            if embedding:
                recs = self.match_by_embedding(df, embedding)
            else:
                recs = self.match_by_skills(df, skills)

            if not recs.empty:
                recs["session_id"] = session_id
                all_recs.append(recs)

        if not all_recs:
            return pd.DataFrame()

        result = pd.concat(all_recs, ignore_index=True)
        result["date_calcul"] = pd.Timestamp.now().date().isoformat()
        logger.success(f"[Recommender] {len(result)} recommandations générées")
        return result
