"""
Gold — KPIs
Calcul des indicateurs clés du marché data
"""
import pandas as pd
from loguru import logger


class KPICalculator:

    def compute_kpi_par_metier(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPIs agrégés par titre de poste normalisé."""
        if df.empty:
            return pd.DataFrame()

        grp = df.groupby("titre_normalise")

        kpis = grp.agg(
            nb_offres    = ("titre_normalise", "count"),
            salaire_moy  = ("salaire_min", "mean"),
            salaire_min  = ("salaire_min", "min"),
            salaire_max  = ("salaire_max", "max"),
            pct_remote   = ("telecommande", "mean"),
        ).reset_index()

        # Top 3 localisations par métier
        top_locs = (
            df[df["localisation"].str.len() > 0]
            .groupby("titre_normalise")["localisation"]
            .apply(lambda x: x.value_counts().head(3).index.tolist())
            .reset_index()
            .rename(columns={"localisation": "top_localisations"})
        )
        kpis = kpis.merge(top_locs, on="titre_normalise", how="left")

        # Top 3 compétences par métier
        def top_skills(group):
            from collections import Counter
            all_skills = []
            for skills in group:
                if isinstance(skills, list):
                    all_skills.extend(skills)
            return [s for s, _ in Counter(all_skills).most_common(3)]

        top_sk = (
            df.groupby("titre_normalise")["competences"]
            .apply(top_skills)
            .reset_index()
            .rename(columns={"competences": "top_competences"})
        )
        kpis = kpis.merge(top_sk, on="titre_normalise", how="left")

        kpis["pct_remote"]  = (kpis["pct_remote"] * 100).round(1)
        kpis["salaire_moy"] = kpis["salaire_moy"].round(0)
        kpis["date_calcul"] = pd.Timestamp.now().date().isoformat()

        logger.success(f"[KPIs] {len(kpis)} métiers analysés")
        return kpis

    def compute_kpi_par_source(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPIs par source de données."""
        if df.empty:
            return pd.DataFrame()

        kpis = df.groupby("source").agg(
            nb_offres   = ("source", "count"),
            pct_remote  = ("telecommande", "mean"),
            salaire_moy = ("salaire_min", "mean"),
        ).reset_index()

        kpis["pct_remote"]  = (kpis["pct_remote"] * 100).round(1)
        kpis["salaire_moy"] = kpis["salaire_moy"].round(0)
        kpis["date_calcul"] = pd.Timestamp.now().date().isoformat()

        logger.success(f"[KPIs] {len(kpis)} sources analysées")
        return kpis

    def compute_tendances_competences(self, df: pd.DataFrame) -> pd.DataFrame:
        """Top compétences globales du marché."""
        if df.empty:
            return pd.DataFrame()

        from collections import Counter
        all_skills = []
        for skills in df["competences"]:
            if isinstance(skills, list):
                all_skills.extend(skills)

        counter = Counter(all_skills)
        tendances = pd.DataFrame(
            counter.most_common(50),
            columns=["competence", "nb_offres"]
        )
        tendances["date_calcul"] = pd.Timestamp.now().date().isoformat()

        logger.success(f"[KPIs] {len(tendances)} compétences analysées")
        return tendances

    def compute_stats_marche(self, df: pd.DataFrame) -> pd.DataFrame:
        """Statistiques globales du marché."""
        if df.empty:
            return pd.DataFrame()

        stats = {
            "date_calcul":   pd.Timestamp.now().date().isoformat(),
            "total_offres":  len(df),
            "offres_cdi":    len(df[df["contrat"].str.contains("CDI", na=False)]),
            "offres_cdd":    len(df[df["contrat"].str.contains("CDD", na=False)]),
            "offres_remote": len(df[df["telecommande"] == True]),
            "nb_metiers":    df["titre_normalise"].nunique(),
            "nb_villes":     df["localisation"].nunique(),
            "salaire_moy":   round(df["salaire_min"].mean(), 0),
        }

        logger.success(f"[KPIs] Stats marché calculées : {stats['total_offres']} offres")
        return pd.DataFrame([stats])
