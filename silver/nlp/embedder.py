"""
Silver — Embedder
Génère les embeddings via le NLP service (sentence-transformers)
Appel HTTP vers le container nlp_service
"""
import os
import requests
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


class Embedder:

    def __init__(self):
        self.nlp_url = os.getenv("NLP_SERVICE_URL", "http://localhost:8001")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Envoie un batch de textes au NLP service et retourne les embeddings."""
        resp = requests.post(
            f"{self.nlp_url}/embed",
            json={"texts": texts},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"]

    def embed(self, df: pd.DataFrame, batch_size: int = 32) -> pd.DataFrame:
        """
        Génère les embeddings pour chaque offre.
        Utilise titre + description tronquée pour le vecteur.
        """
        if df.empty:
            return df

        logger.info(f"[Embedder] Génération des embeddings pour {len(df)} offres...")

        # Prépare les textes : titre + 500 premiers chars de description
        texts = df.apply(
            lambda r: f"{r.get('titre_normalise', '')} {r.get('description', '')[:500]}",
            axis=1,
        ).tolist()

        embeddings = []
        total_batches = (len(texts) + batch_size - 1) // batch_size

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_num = i // batch_size + 1
            try:
                batch_embeddings = self._embed_batch(batch)
                embeddings.extend(batch_embeddings)
                logger.info(f"  → Batch {batch_num}/{total_batches} traité")
            except Exception as e:
                logger.warning(f"  → Batch {batch_num} échoué ({e}) — embeddings None")
                embeddings.extend([None] * len(batch))

        df["embedding"] = embeddings
        nb_ok = sum(1 for e in embeddings if e is not None)
        logger.success(f"[Embedder] {nb_ok}/{len(df)} embeddings générés.")
        return df
