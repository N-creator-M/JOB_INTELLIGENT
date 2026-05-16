"""
Gold — MinIO Handler
Lit le Parquet depuis Silver
Écrit les agrégations en Parquet dans Gold
"""
import os
import io
import json
import boto3
import pandas as pd
from datetime import date
from botocore.client import Config
from loguru import logger


class MinIOHandler:

    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url          = os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id     = os.getenv("MINIO_ROOT_USER", "minio_admin"),
            aws_secret_access_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
            config                = Config(signature_version="s3v4"),
            region_name           = "us-east-1",
        )
        self.bucket_silver = os.getenv("MINIO_BUCKET_SILVER", "silver")
        self.bucket_gold   = os.getenv("MINIO_BUCKET_GOLD",   "gold")
        self._ensure_bucket(self.bucket_gold)

    def _ensure_bucket(self, bucket: str):
        try:
            self.client.head_bucket(Bucket=bucket)
        except Exception:
            self.client.create_bucket(Bucket=bucket)
            logger.info(f"[MinIO] Bucket '{bucket}' créé.")

    # ── Lecture Silver ────────────────────────────────

    def read_silver(self, target_date: str = None) -> pd.DataFrame:
        """Lit le fichier Parquet Silver pour une date donnée."""
        target_date = target_date or date.today().isoformat()
        s3_key = f"{target_date}/offres.parquet"

        try:
            obj    = self.client.get_object(Bucket=self.bucket_silver, Key=s3_key)
            buffer = io.BytesIO(obj["Body"].read())
            df     = pd.read_parquet(buffer, engine="pyarrow")

            # Restaure les colonnes JSON stringifiées
            for col in ["competences", "tags", "area", "embedding"]:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: json.loads(x) if isinstance(x, str) else (x if x is not None else [])
                    )

            logger.info(f"[MinIO] Silver lu : {len(df)} offres depuis {s3_key}")
            return df
        except Exception as e:
            logger.error(f"[MinIO] Impossible de lire Silver {s3_key}: {e}")
            return pd.DataFrame()

    # ── Écriture Gold ─────────────────────────────────

    def write_gold(self, df: pd.DataFrame, name: str) -> str:
        """
        Écrit un DataFrame en Parquet dans le bucket Gold.
        Structure : gold/{YYYY-MM-DD}/{name}.parquet
        """
        if df.empty:
            logger.warning(f"[MinIO] DataFrame vide — rien à écrire pour '{name}'")
            return ""

        today  = date.today().isoformat()
        s3_key = f"{today}/{name}.parquet"

        df_save = df.copy()
        for col in df_save.columns:
            if df_save[col].dtype == object:
                df_save[col] = df_save[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else x
                )

        buffer = io.BytesIO()
        df_save.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.client.put_object(
            Bucket      = self.bucket_gold,
            Key         = s3_key,
            Body        = buffer.getvalue(),
            ContentType = "application/octet-stream",
        )

        size_kb = buffer.getbuffer().nbytes / 1024
        logger.success(
            f"[MinIO] '{name}' écrit → s3://{self.bucket_gold}/{s3_key} ({size_kb:.1f} KB)"
        )
        return f"s3://{self.bucket_gold}/{s3_key}"
