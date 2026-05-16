"""
Silver — MinIO Handler
Lit les fichiers JSON depuis le bucket Bronze
Écrit les fichiers Parquet dans le bucket Silver
"""
import os
import io
import json
import boto3
import pandas as pd
from datetime import date
from datetime import datetime
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
        self.bucket_bronze = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
        self.bucket_silver = os.getenv("MINIO_BUCKET_SILVER", "silver")
        self._ensure_bucket(self.bucket_silver)

    def _ensure_bucket(self, bucket: str):
        """Crée le bucket s'il n'existe pas."""
        try:
            self.client.head_bucket(Bucket=bucket)
        except Exception:
            self.client.create_bucket(Bucket=bucket)
            logger.info(f"[MinIO] Bucket '{bucket}' créé.")

    # ── Lecture Bronze ────────────────────────────────

    def read_bronze(self, target_date: str = None) -> list[dict]:
        """
        Lit tous les fichiers JSON du bucket Bronze pour une date donnée.
        Si target_date est None, lit la date du jour.
        """
        target_date = target_date or date.today().isoformat()
        all_jobs = []

        sources = ["france_travail", "adzuna", "remotive"]

        for source in sources:
            prefix = f"{source}/{target_date}/"
            try:
                resp = self.client.list_objects_v2(
                    Bucket=self.bucket_bronze,
                    Prefix=prefix,
                )
                files = [obj["Key"] for obj in resp.get("Contents", [])]

                for key in files:
                    obj  = self.client.get_object(Bucket=self.bucket_bronze, Key=key)
                    data = json.loads(obj["Body"].read().decode("utf-8"))
                    raw = data.get("jobs", data) 
                    jobs = raw if isinstance(raw, list) else []
                    all_jobs.extend(jobs)
                    logger.info(f"[MinIO] Lus {len(jobs)} offres depuis {key}")

            except Exception as e:
                logger.warning(f"[MinIO] Aucun fichier pour {source}/{target_date}: {e}")
                continue

        logger.info(f"[MinIO] Total Bronze lu : {len(all_jobs)} offres")
        return all_jobs

    # ── Écriture Silver ───────────────────────────────

    def write_silver(self, df: pd.DataFrame) -> str:
        """
        Écrit le DataFrame nettoyé en Parquet dans le bucket Silver.
        Structure : silver/{YYYY-MM-DD}/offres.parquet
        """
        if df.empty:
            logger.warning("[MinIO] DataFrame vide — rien à écrire dans Silver.")
            return ""

        today   = date.today().isoformat()
        s3_key  = f"{today}/offres.parquet"

        # Convertit les listes en strings pour Parquet
        df_save = df.copy()
        for col in ["competences", "tags", "area", "embedding"]:
            if col in df_save.columns:
                df_save[col] = df_save[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else x
                )

        # Sérialise en Parquet en mémoire
        buffer = io.BytesIO()
        df_save.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.client.put_object(
            Bucket      = self.bucket_silver,
            Key         = s3_key,
            Body        = buffer.getvalue(),
            ContentType = "application/octet-stream",
        )

        size_mb = buffer.getbuffer().nbytes / 1024 / 1024
        logger.success(
            f"[MinIO] {len(df)} offres écrites → "
            f"s3://{self.bucket_silver}/{s3_key} ({size_mb:.2f} MB)"
        )
        return f"s3://{self.bucket_silver}/{s3_key}"

    # ── Lecture Silver ────────────────────────────────

    def read_silver(self, target_date: str = None) -> pd.DataFrame:
        """Lit le fichier Parquet Silver pour une date donnée."""
        target_date = target_date or date.today().isoformat()
        s3_key = f"{target_date}/offres.parquet"

        try:
            obj    = self.client.get_object(Bucket=self.bucket_silver, Key=s3_key)
            buffer = io.BytesIO(obj["Body"].read())
            df     = pd.read_parquet(buffer, engine="pyarrow")
            logger.info(f"[MinIO] Silver lu : {len(df)} offres depuis {s3_key}")
            return df
        except Exception as e:
            logger.error(f"[MinIO] Impossible de lire Silver {s3_key}: {e}")
            return pd.DataFrame()
