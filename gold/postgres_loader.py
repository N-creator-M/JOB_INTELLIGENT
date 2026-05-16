"""
Gold — PostgreSQL Loader
Charge les données Gold dans le Star Schema PostgreSQL (gold_db)
Schéma  : gold
Tables  : dim_metier, dim_localisation, dim_source, dim_contrat,
          dim_date, fait_offres
"""
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger


class PostgresLoader:

    def __init__(self):
        self.conn = psycopg2.connect(
            host     = os.getenv("POSTGRES_HOST", "localhost"),
            port     = os.getenv("POSTGRES_PORT", "5432"),
            dbname   = os.getenv("POSTGRES_GOLD_DB", "gold_db"),
            user     = os.getenv("POSTGRES_USER", "ji_user"),
            password = os.getenv("POSTGRES_PASSWORD", "ji_password"),
        )
        self.conn.autocommit = False
        # Pointe vers le schéma gold
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
        self.conn.commit()
        logger.info("[PostgreSQL] Connexion à gold_db établie (schéma: gold).")

    def _execute(self, sql: str, data: list = None):
        with self.conn.cursor() as cur:
            if data:
                execute_values(cur, sql, data)
            else:
                cur.execute(sql)

    # ── Dimensions ────────────────────────────────────

    def load_dim_metier(self, df: pd.DataFrame) -> dict:
        """Insère les métiers et retourne un mapping titre → id."""
        metiers = df["titre_normalise"].dropna().unique()
        mapping = {}

        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
            for titre in metiers:
                cur.execute("""
                    INSERT INTO dim_metier (titre_normalise)
                    VALUES (%s)
                    ON CONFLICT (titre_normalise) DO NOTHING
                    RETURNING id_metier
                """, (titre,))
                row = cur.fetchone()
                if row:
                    mapping[titre] = row[0]
                else:
                    cur.execute(
                        "SELECT id_metier FROM dim_metier WHERE titre_normalise = %s",
                        (titre,)
                    )
                    mapping[titre] = cur.fetchone()[0]

        self.conn.commit()
        logger.info(f"[PostgreSQL] dim_metier : {len(mapping)} métiers chargés")
        return mapping

    def load_dim_localisation(self, df: pd.DataFrame) -> dict:
        """Insère les localisations et retourne un mapping ville → id."""
        locs = df["localisation"].dropna().unique()
        mapping = {}

        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
            for loc in locs:
                cur.execute("""
                    INSERT INTO dim_localisation (ville)
                    VALUES (%s)
                    ON CONFLICT (ville) DO NOTHING
                    RETURNING id_localisation
                """, (loc,))
                row = cur.fetchone()
                if row:
                    mapping[loc] = row[0]
                else:
                    cur.execute(
                        "SELECT id_localisation FROM dim_localisation WHERE ville = %s",
                        (loc,)
                    )
                    mapping[loc] = cur.fetchone()[0]

        self.conn.commit()
        logger.info(f"[PostgreSQL] dim_localisation : {len(mapping)} villes chargées")
        return mapping

    def load_dim_date(self, df: pd.DataFrame) -> dict:
        """Insère les dates et retourne un mapping date → id."""
        mapping = {}

        dates = pd.to_datetime(
            df["date_creation"].fillna(pd.Timestamp.now().date().isoformat()),
            errors="coerce"
        ).dropna().dt.date.unique()

        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
            for d in dates:
                ts = pd.Timestamp(d)
                cur.execute("""
                    INSERT INTO dim_date (date_publication, annee, mois, semaine, jour, trimestre)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_publication) DO NOTHING
                    RETURNING id_date
                """, (
                    d,
                    ts.year,
                    ts.month,
                    ts.isocalendar()[1],
                    ts.day,
                    f"Q{ts.quarter}",
                ))
                row = cur.fetchone()
                if row:
                    mapping[str(d)] = row[0]
                else:
                    cur.execute(
                        "SELECT id_date FROM dim_date WHERE date_publication = %s", (d,)
                    )
                    mapping[str(d)] = cur.fetchone()[0]

        self.conn.commit()
        logger.info(f"[PostgreSQL] dim_date : {len(mapping)} dates chargées")
        return mapping

    def load_sources_mapping(self) -> dict:
        """Récupère le mapping source → id depuis dim_source."""
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
            cur.execute("SELECT nom_source, id_source FROM dim_source")
            return {row[0]: row[1] for row in cur.fetchall()}

    def load_contrats_mapping(self) -> dict:
        """Récupère le mapping contrat → id depuis dim_contrat."""
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
            cur.execute("SELECT type_contrat, id_contrat FROM dim_contrat")
            return {row[0]: row[1] for row in cur.fetchall()}

    # ── Table de faits ────────────────────────────────

    def load_fait_offres(
        self,
        df: pd.DataFrame,
        map_metier: dict,
        map_loc: dict,
        map_source: dict,
        map_contrat: dict,
        map_date: dict,
    ):
        """Insère les offres dans la table de faits fait_offres."""
        rows = []
        for _, row in df.iterrows():
            date_str = str(pd.to_datetime(
                row.get("date_creation"), errors="coerce"
            ).date()) if row.get("date_creation") else None

            rows.append((
                map_metier.get(row.get("titre_normalise")),
                map_loc.get(row.get("localisation")),
                map_source.get(row.get("source")),
                map_contrat.get(row.get("contrat"), map_contrat.get("Non précisé")),
                map_date.get(date_str),
                row.get("titre", ""),
                row.get("entreprise", ""),
                row.get("description", "")[:2000] if row.get("description") else "",
                row.get("salaire_min"),
                row.get("salaire_max"),
                bool(row.get("telecommande", False)),
                row.get("competences") if isinstance(row.get("competences"), list) else [],
                row.get("hash_dedup", ""),
                row.get("url", ""),
            ))

        sql = """
            INSERT INTO fait_offres (
                id_metier, id_localisation, id_source, id_contrat, id_date,
                titre, entreprise, description,
                salaire_min, salaire_max, telecommande,
                competences, hash_dedup, url
            ) VALUES %s
            ON CONFLICT (hash_dedup) DO NOTHING
        """
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO gold")
        self._execute(sql, rows)
        self.conn.commit()
        logger.success(f"[PostgreSQL] fait_offres : {len(rows)} offres insérées")

    def close(self):
        self.conn.close()
        logger.info("[PostgreSQL] Connexion fermée.")