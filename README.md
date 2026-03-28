# Projet Big Data : Rentabilisation des Yellow Cabs de NYC

Membres de l'équipe : Vincent BURGEVIN / Amina MEDJDOUB
Cours : Big Data Framework

## 1. Problématique business

Avec la concurrence des VTC comme Uber, les taxis jaunes de New York (Yellow Cabs) perdent une partie de leur clientèle. L'objectif de ce projet est d'analyser les trajets en fonction des zones, des horaires et des modes de paiement pour identifier des axes d'amélioration de la rentabilité.

## 2. Architecture du pipeline

Nous avons utilisé Hadoop et PySpark sur Docker avec une architecture Médaillon (Bronze → Silver → Gold).

### Couche Raw (Bronze) — `feeder.py`
- Ingère les fichiers sources : trajets (`.parquet`) et référentiel des zones (`taxi_zone_lookup.csv`)
- Partitionne les données par année, mois et jour en se basant sur `tpep_pickup_datetime`
- Écrit les données dans `/raw` sur HDFS
- Génère le log `feeder.txt`

### Couche Silver — `processor.py`
- Lit les données depuis `/raw`
- Applique 5 règles de qualité : prix > 0, distance > 0, dates cohérentes, passagers entre 1 et 6, LocationID valide
- Effectue les jointures avec le référentiel des zones (départ et arrivée) et les types de paiement
- Utilise la window function `RANK()` et `.persist()` pour optimiser Spark
- Écrit les tables nettoyées dans Hive (`/user/hive/warehouse/silver.db/`)
- Génère le log `processor.txt`

### Couche Gold — `datamart.py`
- Lit les données Silver via Spark SQL
- Crée 3 datamarts métier : `dm_zone_performance`, `dm_hourly_demand`, `dm_payment_analysis`
- Exporte ces datamarts dans une base SQLite (`pipeline/datamarts.db`)
- Génère le log `datamart.txt`

### API et Visualisation
- **`api/app.py`** : API FastAPI sécurisée avec JWT, 3 endpoints paginés, log dans `api/app_logs.txt`
- **`api/app_streamlit.py`** : Dashboard de visualisation avec 4 graphiques interactifs connectés à l'API

## 3. Exécution

**1. Démarrer Docker :**
```bash
docker-compose up -d
```

**2. Convertir les fichiers source en format Snappy (compatibilité Spark 3.0) :**
```bash
python pipeline/convertisseur_zstd.py
```

**3. Ingestion vers HDFS (couche Raw) :**
```bash
docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/feeder.py
```

**4. Nettoyage vers Hive (couche Silver) :**
```bash
docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/processor.py
```

**5. Création des datamarts SQLite (couche Gold) :**
```bash
docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/datamart.py
```

**6. Lancer l'API (terminal 1) :**
```bash
cd api
pip install -r requirements.txt
uvicorn app:app --reload
```
API disponible sur [http://localhost:8000/docs](http://localhost:8000/docs)
Identifiants : `admin` / `admin`

Endpoints disponibles :
- `POST /auth/token` — obtenir un token JWT
- `GET /datamarts/zone_performance` — top zones par revenus
- `GET /datamarts/hourly_demand` — demande par heure
- `GET /datamarts/payment_analysis` — répartition des paiements

**7. Lancer le dashboard Streamlit (terminal 2) :**
```bash
cd api
streamlit run app_streamlit.py
```
Le dashboard s'ouvre automatiquement dans le navigateur.

## 4. Choix techniques

- **Mémoire Spark** : Le traitement de tous les fichiers Parquet causait des `OutOfMemoryError`. Nous avons ajouté `.repartition(4)` dans `feeder.py` pour limiter la consommation mémoire lors des shuffles.
- **Doublon de colonne Hive** : La double jointure sur le référentiel des zones dupliquait la colonne `service_zone`, ce qui bloquait l'écriture en format Parquet dans Hive. Nous avons ajouté `.drop("service_zone")` pour contourner ce problème.
- **SQLite pour les datamarts** : Plutôt qu'une base distante, nous avons choisi SQLite pour sa simplicité de déploiement. Le fichier `datamarts.db` est directement lu par l'API FastAPI sans configuration supplémentaire.
