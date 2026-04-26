# Job Intelligent

## À propos

Nous sommes **Nassima Maarouf** et **Fatima Zahra Ben Taleb**, étudiantes en Data Engineering.

Ce projet est notre réalisation personnelle dans le cadre de notre formation. Nous avons conçu et développé **Job Intelligent**, un pipeline de données automatisé qui collecte des offres d'emploi, les analyse avec de l'intelligence artificielle, et produit des statistiques sur le marché de l'emploi.

## Ce que fait le projet

- Collecte automatique des offres d'emploi depuis France Travail, Indeed et LinkedIn
- Nettoyage et enrichissement des données avec du NLP (extraction de compétences, embeddings)
- Calcul de KPIs : métiers les plus demandés, localisations, types de contrat
- Exposition des résultats via une API et Power BI

## Technologies utilisées

- **Apache Airflow** — orchestration du pipeline
- **Docker** — conteneurisation
- **PostgreSQL** — stockage des données
- **MinIO** — stockage des fichiers bruts
- **FastAPI** — API publique
- **sentence-transformers** — analyse NLP
- **Power BI** — visualisation

## Lancer le projet

```bash
git clone https://github.com/N-creator-M/JOB_INTELLIGENT.git
cd JOB_INTELLIGENT
docker-compose build
docker-compose up -d postgres minio
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler nlp_service api
```

## Auteures

- **Nassima Maarouf**
- **Fatima Zahra Ben Taleb**
