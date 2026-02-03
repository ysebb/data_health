# -*- coding: utf-8 -*-
import pathlib
import pandas as pd

IN_PATH = pathlib.Path('outputs') / 'merged_passages_urgences_clean.csv'
OUT_PATH = pathlib.Path('outputs') / 'Data_set_final.csv'

if not IN_PATH.exists():
    raise SystemExit(f'Missing input file: {IN_PATH}')

df = pd.read_csv(IN_PATH)

# Step 1: filter scope
if 'region' in df.columns:
    df = df[df['region'] == 'Île-de-France']
if 'classe_age' in df.columns:
    df = df[df['classe_age'] == 'Tous âges']

# Step 2: cleaning
if 'taux_actes_sos_medecins' in df.columns:
    df['taux_actes_sos_medecins'] = df['taux_actes_sos_medecins'].fillna(0)

if 'taux_passages_urgences' in df.columns:
    df = df.dropna(subset=['taux_passages_urgences'])

if 'date_semaine' in df.columns:
    df['date_semaine'] = pd.to_datetime(df['date_semaine'], errors='coerce')

# Step 3: analytical variables
if 'date_semaine' in df.columns:
    df['annee'] = df['date_semaine'].dt.year
    df['mois'] = df['date_semaine'].dt.month
    df['semaine'] = df['date_semaine'].dt.isocalendar().week.astype('Int64')

if 'taux_hospitalisation' in df.columns and 'taux_passages_urgences' in df.columns:
    df['ratio_hosp'] = df['taux_hospitalisation'] / df['taux_passages_urgences']

OUT_PATH.parent.mkdir(exist_ok=True)
df.to_csv(OUT_PATH, index=False)
print(f'Wrote {OUT_PATH} with {len(df)} rows and {len(df.columns)} columns')
