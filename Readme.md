# Projet Cloud - Validation et chargement de données

Ce projet Python automatise la validation et le chargement de données de transactions depuis un bucket Google Cloud Storage vers une table BigQuery.

## Fonctionnement

Le script effectue les étapes suivantes:

1. **Validation des données:**
   - Lit les fichiers CSV depuis un dossier spécifié dans le bucket.
   - Valide les données en fonction d'un schéma prédéfini (types de données, valeurs requises, etc.).
   - Vérifie la présence de valeurs négatives non autorisées.
   - Déplace les fichiers dans un dossier "error" ou "clean" selon le résultat de la validation.

2. **Chargement des données:**
   - Lit les fichiers CSV du dossier "clean".
   - Convertit la colonne `date` au format datetime.
   - Charge les données dans une table BigQuery spécifiée.
   - Déplace les fichiers chargés dans un dossier "done".

## Configuration

Avant d'exécuter le script, vous devez:

* **Créer un bucket Google Cloud Storage** et y placer vos fichiers CSV dans un dossier "input".
* **Créer un dataset et une table BigQuery** avec le schéma approprié.
* **Installer les bibliothèques Python requises:**
