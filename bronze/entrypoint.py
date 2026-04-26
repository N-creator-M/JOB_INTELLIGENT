import argparse
import json
import datetime
import os
import boto3
import requests

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio_admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_ji"),
    )

def upload_to_minio(data, source):
    client = get_minio_client()
    bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    today = datetime.date.today().isoformat()
    key = f"{source}/{today}/offres.json"
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json"
    )
    print(f"[{source}] {len(data)} offres uploadées dans MinIO : {key}")

def scrape_france_travail():
    client_id = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
    client_secret = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")
    if not client_id or client_id == "YOUR_CLIENT_ID":
        print("[france_travail] Clés API manquantes — données de test utilisées")
        return [
            {
                "source": "france_travail",
                "titre": "Data Engineer",
                "entreprise": "Entreprise Test",
                "localisation": "Paris",
                "contrat": "CDI",
                "description": "Nous recherchons un Data Engineer maîtrisant Python, SQL, Spark et Airflow.",
                "url": "https://francetravail.fr/offre/test-1"
            }
        ]
    token_url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
    token_res = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "api_offresdemploiv2 o2dsoffre"
    })
    token = token_res.json().get("access_token")
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(
        "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search",
        headers=headers,
        params={"motsCles": "data engineer", "maxCreationDate": "P7D", "range": "0-49"}
    )
    offres = res.json().get("resultats", [])
    return [
        {
            "source": "france_travail",
            "titre": o.get("intitule", ""),
            "entreprise": o.get("entreprise", {}).get("nom", ""),
            "localisation": o.get("lieuTravail", {}).get("libelle", ""),
            "contrat": o.get("typeContrat", ""),
            "description": o.get("description", ""),
            "url": o.get("origineOffre", {}).get("urlOrigine", "")
        }
        for o in offres
    ]

def scrape_indeed():
    print("[indeed] Pas d'API officielle — données de test utilisées")
    return [
        {
            "source": "indeed",
            "titre": "Data Scientist",
            "entreprise": "Tech Company",
            "localisation": "Lyon",
            "contrat": "CDI",
            "description": "Poste de Data Scientist avec expertise en Python, pandas, scikit-learn et PowerBI.",
            "url": "https://indeed.com/offre/test-1"
        }
    ]

def scrape_linkedin():
    print("[linkedin] Scraping LinkedIn non disponible — données de test utilisées")
    return [
        {
            "source": "linkedin",
            "titre": "Data Analyst",
            "entreprise": "Consulting Firm",
            "localisation": "Bordeaux",
            "contrat": "CDD",
            "description": "Analyste de données avec maîtrise de SQL, Excel, Tableau et Azure.",
            "url": "https://linkedin.com/jobs/test-1"
        }
    ]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["indeed", "france_travail", "linkedin", "all"], default="all")
    args = parser.parse_args()

    if args.source in ("france_travail", "all"):
        data = scrape_france_travail()
        upload_to_minio(data, "france_travail")

    if args.source in ("indeed", "all"):
        data = scrape_indeed()
        upload_to_minio(data, "indeed")

    if args.source in ("linkedin", "all"):
        data = scrape_linkedin()
        upload_to_minio(data, "linkedin")