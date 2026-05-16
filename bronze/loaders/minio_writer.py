"""
MinIO Writer — couche Bronze
Écrit les offres brutes en JSON dans MinIO (Data Lake)
Structure : bronze/{source}/{date}/offres.json
"""
import os
import json
import boto3
from datetime import datetime, date
from botocore.client import Config
from loguru import logger


class MinIOWriter:

    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url          = os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id     = os.getenv("MINIO_ROOT_USER", "minio_admin"),
            aws_secret_access_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
            config                = Config(signature_version="s3v4"),
            region_name           = "us-east-1",
        )
        self.bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Crée le bucket bronze s'il n'existe pas."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception:
            self.client.create_bucket(Bucket=self.bucket)
            logger.info(f"[MinIO] Bucket '{self.bucket}' créé.")

    def write(self, jobs: list[dict], source: str) -> str:
        """
        Écrit les offres dans MinIO.
        Retourne le chemin S3 du fichier écrit.

        Structure : bronze/{source}/{YYYY-MM-DD}/offres_{HH-MM}.json
        """
        if not jobs:
            logger.warning(f"[MinIO] Aucune offre à écrire pour source='{source}'")
            return ""

        today     = date.today().isoformat()
        now       = datetime.utcnow().strftime("%H-%M")
        s3_key    = f"{source}/{today}/offres_{now}.json"

        payload = {
            "source":       source,
            "date":         today,
            "count":        len(jobs),
            "scraped_at":   datetime.utcnow().isoformat(),
            "jobs":         jobs,
        }

        body = json.dumps(payload, ensure_ascii=False, indent=2)

        self.client.put_object(
            Bucket      = self.bucket,
            Key         = s3_key,
            Body        = body.encode("utf-8"),
            ContentType = "application/json",
        )

        logger.success(f"[MinIO] {len(jobs)} offres écrites → s3://{self.bucket}/{s3_key}")
        return f"s3://{self.bucket}/{s3_key}"

    def list_files(self, source: str = "", date_str: str = "") -> list[str]:
        """Liste les fichiers dans le bucket bronze."""
        prefix = source
        if date_str:
            prefix = f"{source}/{date_str}"

        resp  = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        files = [obj["Key"] for obj in resp.get("Contents", [])]
        return files

    def read(self, s3_key: str) -> dict:
        """Lit un fichier JSON depuis le bucket bronze."""
        resp = self.client.get_object(Bucket=self.bucket, Key=s3_key)
        return json.loads(resp["Body"].read().decode("utf-8"))
