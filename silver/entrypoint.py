import argparse
import json
import hashlib
import os
import boto3
import requests
from sqlalchemy import create_engine, text

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio_admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_ji"),
    )

def get_db():
    url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','ji_user')}:{os.getenv('POSTGRES_PASSWORD','ji_123')}@{os.getenv('POSTGRES_HOST','postgres')}:5432/{os.getenv('POSTGRES_SILVER_DB','silver_db')}"
    return create_engine(url)

def get_nlp_url():
    return os.getenv("NLP_SERVICE_URL", "http://nlp_service:8001")

def clean(offres):
    cleaned = []
    for o in offres:
        if not o.get("titre") or not o.get("description"):
            continue
        hash_val = hashlib.sha256(
            f"{o.get('titre','')}{o.get('entreprise','')}{o.get('localisation','')}".encode()
        ).hexdigest()
        o["hash_dedup"] = hash_val
        cleaned.append(o)
    print(f"[clean] {len(cleaned)} offres nettoyées")
    return cleaned

def extract_skills(offres):
    nlp_url = get_nlp_url()
    for o in offres:
        try:
            res = requests.post(f"{nlp_url}/extract-skills", params={"text": o.get("description", "")})
            o["competences"] = res.json().get("skills", [])
        except Exception as e:
            print(f"[nlp] Erreur extraction skills : {e}")
            o["competences"] = []
    print(f"[nlp] Skills extraits pour {len(offres)} offres")
    return offres

def generate_embeddings(offres):
    nlp_url = get_nlp_url()
    texts = [o.get("description", "") for o in offres]
    try:
        res = requests.post(f"{nlp_url}/embed", json=texts, headers={"Content-Type": "application/json"})
        embeddings = res.json().get("embeddings", [])
        for i, o in enumerate(offres):
            o["embedding"] = embeddings[i] if i < len(embeddings) else []
    except Exception as e:
        print(f"[nlp] Erreur embeddings : {e}")
        for o in offres:
            o["embedding"] = []
    print(f"[embed] Embeddings générés pour {len(offres)} offres")
    return offres

def save_to_postgres(offres):
    engine = get_db()
    with engine.begin() as conn:
        for o in offres:
            conn.execute(text("""
                INSERT INTO silver.offres
                    (source, titre, titre_normalise, entreprise, localisation,
                     contrat, description, competences, url, hash_dedup, embedding, date_publication)
                VALUES
                    (:source, :titre, :titre, :entreprise, :localisation,
                     :contrat, :description, :competences, :url, :hash_dedup, :embedding, CURRENT_DATE)
                ON CONFLICT (hash_dedup) DO NOTHING
            """), {
                "source": o.get("source", ""),
                "titre": o.get("titre", ""),
                "entreprise": o.get("entreprise", ""),
                "localisation": o.get("localisation", ""),
                "contrat": o.get("contrat", ""),
                "description": o.get("description", ""),
                "competences": o.get("competences", []),
                "url": o.get("url", ""),
                "hash_dedup": o.get("hash_dedup", ""),
                "embedding": o.get("embedding", []),
            })
    print(f"[postgres] {len(offres)} offres insérées dans silver.offres")

def load_from_minio():
    client = get_minio_client()
    bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    offres = []
    try:
        objects = client.list_objects_v2(Bucket=bucket)
        for obj in objects.get("Contents", []):
            body = client.get_object(Bucket=bucket, Key=obj["Key"])
            data = json.loads(body["Body"].read())
            offres.extend(data)
    except Exception as e:
        print(f"[minio] Erreur lecture : {e}")
    print(f"[minio] {len(offres)} offres chargées depuis Bronze")
    return offres

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["clean", "nlp", "embed", "save", "all"], default="all")
    args = parser.parse_args()

    offres = load_from_minio()
    offres = clean(offres)

    if args.step in ("nlp", "all"):
        offres = extract_skills(offres)

    if args.step in ("embed", "all"):
        offres = generate_embeddings(offres)

    if args.step in ("save", "all"):
        save_to_postgres(offres)