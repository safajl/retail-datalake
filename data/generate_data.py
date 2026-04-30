# ============================================================
# Génération des données -- Data Lake Retail
# Master FAVD -- Visualisation des Données Massives
# ============================================================

import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker('fr_FR')
random.seed(42)
np.random.seed(42)

# ── Données de référence ─────────────────────────────────────
CATEGORIES = {
    'Électronique':  ['Smartphones', 'Laptops', 'Tablettes', 'TV'],
    'Vêtements':     ['Homme', 'Femme', 'Enfant', 'Sport'],
    'Alimentation':  ['Épicerie', 'Boissons', 'Boulangerie', 'Bio'],
    'Maison':        ['Meubles', 'Décoration', 'Cuisine', 'Jardin'],
    'Sport':         ['Fitness', 'Football', 'Natation', 'Cyclisme'],
}

GOVERNORATES = [
    'Tunis', 'Sfax', 'Sousse', 'Nabeul', 'Bizerte',
    'Gabès', 'Monastir', 'Gafsa', 'Kairouan', 'Béja',
    'Ariana', 'Ben Arous', 'Manouba', 'Zaghouan', 'Siliana',
]

REGIONS = {
    'Tunis': 'Nord', 'Ariana': 'Nord', 'Ben Arous': 'Nord',
    'Manouba': 'Nord', 'Bizerte': 'Nord', 'Nabeul': 'Nord',
    'Zaghouan': 'Nord', 'Béja': 'Nord', 'Siliana': 'Nord',
    'Sousse': 'Centre', 'Monastir': 'Centre',
    'Sfax': 'Centre', 'Kairouan': 'Centre',
    'Gabès': 'Sud', 'Gafsa': 'Sud',
}

STORE_TYPES = ['Hypermarché', 'Supermarché', 'Magasin de proximité']
SEGMENTS    = ['Gold', 'Silver', 'Bronze']
AGE_GROUPS  = ['18-25', '26-35', '36-50', '50+']
BRANDS      = ['TunisiaShop', 'RetailTN', 'MedStore', 'SudMarket', 'NordPlus']


def generate_sales(n=100000):
    """Génère 100 000 transactions de vente."""
    start = datetime(2022, 1, 1)
    sales = []

    for i in range(n):
        date     = start + timedelta(days=random.randint(0, 1095))
        cat      = random.choice(list(CATEGORIES.keys()))
        sub_cat  = random.choice(CATEGORIES[cat])
        gov      = random.choice(GOVERNORATES)
        qty      = random.randint(1, 20)
        price    = round(random.uniform(5, 800), 2)
        discount = round(random.uniform(0, 0.3), 2)
        cost     = round(price * random.uniform(0.4, 0.7), 2)
        total_ht = round(qty * price * (1 - discount), 2)
        total_ttc = round(total_ht * 1.19, 2)
        margin   = round(total_ht - qty * cost, 2)

        sales.append({
            'sale_id':      i + 1,
            'date':         date.strftime('%Y-%m-%d'),
            'year':         date.year,
            'month':        date.month,
            'quarter':      (date.month - 1) // 3 + 1,
            'product_name': f'{fake.word().title()} {sub_cat}',
            'category':     cat,
            'sub_category': sub_cat,
            'brand':        random.choice(BRANDS),
            'store_name':   f'RetailTN {gov} {random.randint(1,5)}',
            'city':         fake.city(),
            'governorate':  gov,
            'region':       REGIONS.get(gov, 'Centre'),
            'store_type':   random.choice(STORE_TYPES),
            'customer_name': fake.name(),
            'age_group':    random.choice(AGE_GROUPS),
            'segment':      random.choice(SEGMENTS),
            'quantity':     qty,
            'unit_price':   price,
            'discount_pct': round(discount * 100, 2),
            'total_ht':     total_ht,
            'total_ttc':    total_ttc,
            'cost_price':   cost,
            'gross_margin': margin,
        })

    return pd.DataFrame(sales)


if __name__ == '__main__':
    print('Génération des données en cours...')
    os.makedirs('raw', exist_ok=True)

    print('  → 100 000 transactions de vente...')
    df = generate_sales(100000)
    df.to_csv('raw/ventes_retail.csv', index=False)

    print(f'Données générées avec succès !')
    print(f'  Fichier : data/raw/ventes_retail.csv')
    print(f'  Lignes  : {len(df):,}')
    print(f'  Colonnes: {len(df.columns)}')
    print(f'\nAperçu :')
    print(df.head(3).to_string())