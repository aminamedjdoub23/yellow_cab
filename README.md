# Projet Big Data : Rentabilisation des Yellow Cabs de NYC

Membres de l'équipe : Vincent BURGEVIN / Amina MEDJDOUB
Cours : Big Data Framework

## 1. Problématique business
Avec la concurrence de services comme UBER et les VTC, les taxis jaunes de New York (Yellow Cabs) perdent une partie de leur clientèle. L'objectif de notre projet est d'analyser les trajets en fonction des zones, des horaires et des modes de paiements pour essayer de trouver des axes d'amélioration de la rentabilité.

## 2. Architecture du pipeline

Nous avons utilisé Hadoop et PySpark sur Docker avec une architecture de type Medaillon (Bronze, Silver, Gold).

### Partie 1 : Data Engineering (couches Raw et Silver)

- **Le script `feeder.py` (Couche Bronze / Raw)**
  - Il ingère les fichiers sources : les trajets (en `.parquet`) et le fichier de référence des zones (`taxi_zone_lookup.csv`).
  - Il partitionne les données par année, mois et jour (ex: `year=2024/month=01/day=01`) en se basant sur la date de départ (`tpep_pickup_datetime`).
  - Il écrit les données dans le dossier `/raw` sur HDFS.
  - Il génère un fichier de log `feeder.txt`.

- **Le script `processor.py` (Couche Silver)**
  - Il lit les données stockées dans `/raw`.
  - Il nettoie les données avec 5 grandes règles qualité : 
    - Le prix est supérieur à zéro (`fare_amount > 0`).
    - La distance est supérieure à zéro (`trip_distance > 0`).
    - Les dates de départ et d'arrivée sont logiques.
    - Le nombre de passagers est compris entre 1 et 6 passagers pour enlever les valeurs aberrantes.
    - Les *LocationID* existent bien dans le fichier de référence.
  - Il fait les jointures avec le fichier des zones (une fois pour le point de départ, une fois pour l'arrivée) et le fichier des types de paiement.
  - Il calcule les agrégations : revenu moyen par zone et par heure, et nombre de courses par jour et par borough.
  - Il utilise la fonction `RANK()` (window function) pour classer les revenus, et la méthode `.persist()` pour optimiser la mémoire de Spark.
  - Il écrit les tables nettoyées dans la base de données Hive (`/user/hive/warehouse/silver.db/`) en les partitionnant par jour ou par zone pour optimiser les requêtes futures.
  - Il crée un log `processor.txt`.

### Partie 2 : Datamarts et API (Couche Gold) - Travail du binôme
- **Le script `datamart.py`**
  - Il lit les données de la couche Silver via Spark SQL.
  - Il crée 3 tables d'analyse (Datamarts) orientées métier : `dm_zone_performance`, `dm_hourly_demand`, et `dm_payment_analysis`.
  - Il écrit ces datamarts dans une base relationnelle.
  - Il crée le log `datamart.txt`.
- **L'API FastAPI**
  - Elle permet d'accéder aux données finales avec une authentification JWT sécurisée.

## 3. Execution

1. Démarrer Docker :
```bash
docker-compose up -d
```

2. Convertir le fichier de base en `snappy` (Correction de compatibilité Spark 3.0) :
```bash
python pipeline/convertisseur_zstd.py
```

3. Lancer l'ingestion vers le dossier Raw (HDFS) :
```bash
docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/feeder.py
```

4. Lancer le nettoyage vers la base Silver (Hive) :
```bash
docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/processor.py
```

## 4. Choix techniques rencontrés lors du projet
- **Gestion de la mémoire (RAM)** : L'ingestion des énormes fichiers Parquet faisait crasher notre noeud Spark (`OutOfMemoryError: Java heap space`). Plutôt que de faire une boucle Python (ce qui casserait la logique distribuée), nous avons optimisé la gestion des partitions de Spark. Nous avons ajouté la commande `.repartition(4)` juste avant l'écriture dans `feeder.py`. Cela force Spark à regrouper les données en 4 blocs de traitement en mémoire, ce qui allège la RAM lors du shuffle massif et règle les crashs d'écriture sur HDFS !
- **Le plantage au niveau de Hive (`service_zone`)** : Lors de notre double jointure sur le fichier des zones, la colonne `service_zone` s'est retrouvée copiée en double dans notre tableau final. Cela empêchait Hive de sauvegarder les données au format Parquet. Nous avons simplement ajouté une commande `.drop("service_zone")` dans Spark pour retirer cette colonne avant la jointure, car elle n'était de toute façon pas demandée pour nos KPI de rentabilité.
