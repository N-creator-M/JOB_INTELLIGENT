"""
Job Intelligent — FastAPI Backend
Endpoints :
  GET  /          → Interface web
  POST /recommend → Recommandation par skills
  POST /extract-cv → Extraction skills depuis PDF
  GET  /docs      → Swagger UI
"""
import os
import io
import json
import boto3
import pandas as pd
import numpy as np
from collections import Counter
from datetime import date
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from botocore.client import Config
from loguru import logger

# ── App ───────────────────────────────────────────
app = FastAPI(
    title="Job Intelligent API",
    description="Système de recommandation d'offres data basé sur votre profil",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── MinIO client ──────────────────────────────────
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url          = os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id     = os.getenv("MINIO_ROOT_USER", "minio_admin"),
        aws_secret_access_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_ji"),
        config                = Config(signature_version="s3v4"),
        region_name           = "us-east-1",
    )

# ── Chargement Silver ─────────────────────────────
def load_silver() -> pd.DataFrame:
    """Charge les offres Silver depuis MinIO."""
    client = get_minio_client()
    today  = date.today().isoformat()
    key    = f"{today}/offres.parquet"

    try:
        obj    = client.get_object(Bucket="silver", Key=key)
        buffer = io.BytesIO(obj["Body"].read())
        df     = pd.read_parquet(buffer, engine="pyarrow")

        for col in ["competences", "tags", "area", "embedding"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else (x or [])
                )
        return df
    except Exception as e:
        logger.error(f"Erreur chargement Silver: {e}")
        return pd.DataFrame()

# ── Extraction skills ─────────────────────────────
SKILLS_DICT = {
    "python", "sql", "r", "scala", "java", "go",
    "spark", "pyspark", "kafka", "airflow", "dbt", "databricks",
    "aws", "azure", "gcp", "s3", "bigquery", "snowflake", "redshift",
    "postgresql", "mongodb", "redis", "elasticsearch",
    "tensorflow", "pytorch", "scikit-learn", "xgboost", "lightgbm",
    "mlflow", "hugging face", "transformers", "llm", "rag",
    "docker", "kubernetes", "git", "terraform",
    "power bi", "tableau", "looker", "grafana",
    "machine learning", "deep learning", "nlp", "computer vision",
    "data lake", "data warehouse", "etl", "elt", "parquet", "delta lake",
}

def extract_skills_from_text(text: str) -> list[str]:
    """Extrait les compétences d'un texte."""
    import re
    text_lower = text.lower()
    found = set()
    for skill in SKILLS_DICT:
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, text_lower):
            found.add(skill)
    return sorted(list(found))

# ── Recommandation par Jaccard ────────────────────
def recommend_by_skills(
    df: pd.DataFrame,
    candidate_skills: list[str],
    top_n: int = 10,
) -> list[dict]:
    """Recommande des offres par similarité Jaccard sur les compétences."""
    if df.empty or not candidate_skills:
        return []

    candidate_set = set(s.lower() for s in candidate_skills)

    def score(offre_skills):
        if not isinstance(offre_skills, list) or not offre_skills:
            return 0.0
        offre_set = set(s.lower() for s in offre_skills)
        intersection = candidate_set & offre_set
        union = candidate_set | offre_set
        return len(intersection) / len(union) if union else 0.0

    df = df.copy()
    df["score"] = df["competences"].apply(score)
    df = df[df["score"] > 0].sort_values("score", ascending=False).head(top_n)

    results = []
    for i, (_, row) in enumerate(df.iterrows()):
        results.append({
            "rang":          i + 1,
            "score":         round(float(row["score"]), 3),
            "titre":         row.get("titre", ""),
            "titre_normalise": row.get("titre_normalise", ""),
            "entreprise":    row.get("entreprise", ""),
            "localisation":  row.get("localisation", ""),
            "contrat":       row.get("contrat", ""),
            "salaire_min":   row.get("salaire_min"),
            "salaire_max":   row.get("salaire_max"),
            "telecommande":  bool(row.get("telecommande", False)),
            "competences":   row.get("competences", [])[:8],
            "url":           row.get("url", ""),
            "source":        row.get("source", ""),
        })
    return results

# ── Modèles Pydantic ──────────────────────────────
class RecommendRequest(BaseModel):
    skills: list[str]
    top_n:  int = 10

class RecommendResponse(BaseModel):
    nb_offres:        int
    skills_detectes:  list[str]
    recommandations:  list[dict]

# ── Endpoints ─────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home():
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base_dir, "templates", "index.html"), "r", encoding="utf-8") as f:
        return f.read()

@app.post("/recommend", response_model=RecommendResponse)
async def recommend(req: RecommendRequest):
    """
    Recommande des offres basées sur une liste de compétences.
    """
    df = load_silver()
    if df.empty:
        raise HTTPException(status_code=503, detail="Données non disponibles")

    results = recommend_by_skills(df, req.skills, req.top_n)
    return {
        "nb_offres":       len(df),
        "skills_detectes": req.skills,
        "recommandations": results,
    }

@app.post("/extract-cv")
async def extract_cv(file: UploadFile = File(None), text: str = Form(None)):
    """
    Extrait les compétences depuis un PDF ou du texte libre.
    """
    extracted_text = ""

    if file and file.filename.endswith(".pdf"):
        try:
            import pdfplumber
            content = await file.read()
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    extracted_text += page.extract_text() or ""
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Erreur lecture PDF: {e}")

    elif text:
        extracted_text = text
    else:
        raise HTTPException(status_code=400, detail="Fournir un fichier PDF ou du texte")

    skills = extract_skills_from_text(extracted_text)

    if not skills:
        return {"skills": [], "message": "Aucune compétence data détectée"}

    # Recommandations automatiques
    df      = load_silver()
    results = recommend_by_skills(df, skills, top_n=10)

    return {
        "skills":          skills,
        "nb_skills":       len(skills),
        "recommandations": results,
        "nb_offres_total": len(df),
    }

@app.get("/stats")
async def stats():
    """Statistiques globales du marché data."""
    df = load_silver()
    if df.empty:
        return {"error": "Données non disponibles"}

    all_skills = []
    for skills in df["competences"]:
        if isinstance(skills, list):
            all_skills.extend(skills)

    return {
        "total_offres":    len(df),
        "nb_metiers":      df["titre_normalise"].nunique() if "titre_normalise" in df.columns else 0,
        "nb_villes":       df["localisation"].nunique(),
        "top_competences": [s for s, _ in Counter(all_skills).most_common(10)],
        "top_metiers":     df["titre_normalise"].value_counts().head(10).to_dict() if "titre_normalise" in df.columns else {},
        "sources":         df["source"].value_counts().to_dict(),
    }
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base_dir, "templates", "dashboard.html"), "r", encoding="utf-8") as f:
        return f.read()