import pathlib
from datetime import date

import pandas as pd

BASE = pathlib.Path("CSV")
FILES = sorted(BASE.glob("*.csv"))
if not FILES:
    raise SystemExit("No CSV files found in CSV/")


def reason_from_name(name: str) -> str:
    # Strip common suffixes to derive the reason label from the filename.
    suffixes = [
        "-passages-aux-urgences-et-actes-sos-medecins-region.csv",
        "-passages-aux-urgences-region.csv",
        "-passages-urgences-et-actes-sos-medecin_reg.csv",
    ]
    for suffix in suffixes:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name[:-4] if name.lower().endswith(".csv") else name


def iso_week_to_date(week_str: str):
    # Convert "YYYY-Sww" to the Monday of that ISO week.
    if not isinstance(week_str, str):
        return pd.NaT
    try:
        year_part, week_part = week_str.split("-S")
        year = int(year_part)
        week = int(week_part)
        return pd.Timestamp(date.fromisocalendar(year, week, 1))
    except Exception:
        return pd.NaT


def pick_first_column(df: pd.DataFrame, prefix: str) -> str | None:
    for col in df.columns:
        if col.startswith(prefix):
            return col
    return None


frames = []
for f in FILES:
    df = pd.read_csv(f)
    df["raison"] = reason_from_name(f.name)

    # Date: prefer "1er jour de la semaine", else derive from "Semaine", else from "Année".
    if "1er jour de la semaine" in df.columns:
        df["date_semaine"] = pd.to_datetime(df["1er jour de la semaine"], errors="coerce")
    elif "Semaine" in df.columns:
        df["date_semaine"] = df["Semaine"].apply(iso_week_to_date)
    elif "Année" in df.columns:
        df["date_semaine"] = pd.to_datetime(df["Année"].astype("Int64").astype(str) + "-01-01", errors="coerce")
    else:
        df["date_semaine"] = pd.NaT

    # Standard dimensions.
    if "Région" in df.columns:
        df["region"] = df["Région"]
    else:
        df["region"] = pd.NA

    if "Classe d'âge" in df.columns:
        df["classe_age"] = df["Classe d'âge"]
    else:
        df["classe_age"] = pd.NA

    # Standard measures (prefix search).
    passages_col = pick_first_column(df, "Taux de passages aux urgences pour ")
    hospit_col = pick_first_column(df, "Taux d'hospitalisations après passages aux urgences pour ")
    actes_col = pick_first_column(df, "Taux d'actes médicaux SOS médecins pour ")

    df["taux_passages_urgences"] = df[passages_col] if passages_col else pd.NA
    df["taux_hospitalisation"] = df[hospit_col] if hospit_col else pd.NA
    df["taux_actes_sos_medecins"] = df[actes_col] if actes_col else pd.NA

    frames.append(
        df[
            [
                "date_semaine",
                "region",
                "classe_age",
                "taux_passages_urgences",
                "taux_hospitalisation",
                "taux_actes_sos_medecins",
                "raison",
            ]
        ]
    )


merged = pd.concat(frames, ignore_index=True)

out_dir = pathlib.Path("outputs")
out_dir.mkdir(exist_ok=True)
out_path = out_dir / "merged_passages_urgences_clean.csv"
merged.to_csv(out_path, index=False)
print(f"Wrote {out_path} with {len(merged)} rows and {len(merged.columns)} columns")
