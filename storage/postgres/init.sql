-- =====================================================
-- JOB INTELLIGENT — init.sql
-- Exécuté automatiquement au démarrage Docker
-- =====================================================

-- ── Création des bases ────────────────────────────
CREATE DATABASE gold_db;

-- =====================================================
-- GOLD DB — Star Schema uniquement
-- Silver est stocké dans MinIO (Parquet)
-- =====================================================
\connect gold_db;

CREATE SCHEMA IF NOT EXISTS gold;

-- ── Dimensions ────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_metier (
    id_metier        SERIAL PRIMARY KEY,
    titre_normalise  VARCHAR(100) NOT NULL UNIQUE,
    categorie        VARCHAR(100),
    domaine          VARCHAR(100),
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_localisation (
    id_localisation  SERIAL PRIMARY KEY,
    ville            VARCHAR(150) UNIQUE,
    departement      VARCHAR(100),
    region           VARCHAR(100),
    pays             VARCHAR(100) DEFAULT 'France',
    code_postal      VARCHAR(10),
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_source (
    id_source        SERIAL PRIMARY KEY,
    nom_source       VARCHAR(50) NOT NULL UNIQUE,
    url_source       VARCHAR(255),
    type_source      VARCHAR(50),
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_contrat (
    id_contrat       SERIAL PRIMARY KEY,
    type_contrat     VARCHAR(50) NOT NULL UNIQUE,
    libelle          VARCHAR(100),
    famille          VARCHAR(50),
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    id_date          SERIAL PRIMARY KEY,
    date_publication DATE NOT NULL UNIQUE,
    annee            INT,
    mois             INT,
    semaine          INT,
    jour             INT,
    trimestre        VARCHAR(10),
    created_at       TIMESTAMP DEFAULT NOW()
);

-- ── Table de faits ────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.fait_offres (
    id_offre         SERIAL PRIMARY KEY,
    id_metier        INT REFERENCES gold.dim_metier(id_metier),
    id_localisation  INT REFERENCES gold.dim_localisation(id_localisation),
    id_source        INT REFERENCES gold.dim_source(id_source),
    id_contrat       INT REFERENCES gold.dim_contrat(id_contrat),
    id_date          INT REFERENCES gold.dim_date(id_date),
    titre            VARCHAR(255),
    entreprise       VARCHAR(255),
    description      TEXT,
    salaire_min      NUMERIC(10,2),
    salaire_max      NUMERIC(10,2),
    telecommande     BOOLEAN DEFAULT FALSE,
    competences      TEXT[],
    embedding        FLOAT8[],
    hash_dedup       VARCHAR(64) UNIQUE,
    url              TEXT,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- ── Index ─────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fait_metier       ON gold.fait_offres(id_metier);
CREATE INDEX IF NOT EXISTS idx_fait_localisation ON gold.fait_offres(id_localisation);
CREATE INDEX IF NOT EXISTS idx_fait_source       ON gold.fait_offres(id_source);
CREATE INDEX IF NOT EXISTS idx_fait_contrat      ON gold.fait_offres(id_contrat);
CREATE INDEX IF NOT EXISTS idx_fait_date         ON gold.fait_offres(id_date);
CREATE INDEX IF NOT EXISTS idx_fait_telecommande ON gold.fait_offres(telecommande);

-- ── Données de référence ──────────────────────────

INSERT INTO gold.dim_source (nom_source, url_source, type_source) VALUES
    ('france_travail', 'https://francetravail.io', 'API officielle'),
    ('adzuna',         'https://api.adzuna.com',   'API gratuite'),
    ('remotive',       'https://remotive.com',     'API publique')
ON CONFLICT (nom_source) DO NOTHING;

INSERT INTO gold.dim_contrat (type_contrat, libelle, famille) VALUES
    ('CDI',         'Contrat à durée indéterminée', 'Permanent'),
    ('CDD',         'Contrat à durée déterminée',   'Temporaire'),
    ('Freelance',   'Freelance / Indépendant',       'Indépendant'),
    ('Stage',       'Stage',                         'Formation'),
    ('Alternance',  'Alternance / Apprentissage',    'Formation'),
    ('Intérim',     'Mission intérimaire',           'Temporaire'),
    ('Non précisé', 'Non précisé',                  'Autre')
ON CONFLICT (type_contrat) DO NOTHING;