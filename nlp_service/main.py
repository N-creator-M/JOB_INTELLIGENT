from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import re

app = FastAPI()

model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

SKILLS = [
    "python", "sql", "spark", "airflow", "docker", "kafka",
    "dbt", "pandas", "numpy", "scikit-learn", "tensorflow",
    "pytorch", "powerbi", "tableau", "excel", "azure", "aws",
    "gcp", "postgresql", "mongodb", "redis", "elasticsearch"
]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/embed")
def embed(texts: list[str]):
    vectors = model.encode(texts).tolist()
    return {"embeddings": vectors}

@app.post("/extract-skills")
def extract_skills(text: str):
    found = [s for s in SKILLS if re.search(r'\b' + s + r'\b', text.lower())]
    return {"skills": found}