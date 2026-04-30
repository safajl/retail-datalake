# 🏪 Retail Data Lake — Master FAVD

## Description
Data Lake complet pour l'analyse des ventes retail en Tunisie.
100 000 transactions générées sur 3 ans (2022-2024).

## Architecture
- **HDFS** — Stockage distribué des données brutes
- **Apache Spark** — Traitement et calcul des KPIs
- **Jupyter** — Visualisation et analyse

## Stack technique
- Hadoop 3.2.1
- Spark 3.3.0
- Python 3.12
- Docker & Docker Compose

## Lancer le projet
```bash
docker-compose -f docker/docker-compose.yml up -d
python data/generate_data.py
```

## KPIs calculés
- CA par magasin et région
- Évolution mensuelle 2022-2024
- Top produits par CA et marge
- Impact des promotions