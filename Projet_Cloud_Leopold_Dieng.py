#!/usr/bin/env python
# coding: utf-8

# In[14]:


from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from io import BytesIO

# Paramètres de configuration
nom_projet = "isi-group-m2-dsia"
nom_bucket = "m2dsia-dieng-leopold-data"
nom_table = "isi-group-m2-dsia.dataset_dieng_leopold.transactions"


# Initialisation des clients Storage et BigQuery
client_storage = storage.Client(project=nom_projet)
bucket = client_storage.bucket(nom_bucket)
client_bigquery = bigquery.Client(project=nom_projet)


# Liste des fichiers dans le dossier 'test_input/'
liste_fichiers = bucket.list_blobs(prefix='input/')

for fichier_csv in liste_fichiers:
    if fichier_csv.name.endswith('.csv'):  # Filtrer uniquement les fichiers CSV
        print(f"---------------- Traitement du fichier: {fichier_csv.name}")

        # Lecture du fichier CSV depuis le bucket
        contenu_fichier = fichier_csv.download_as_bytes()
        df = pd.read_csv(BytesIO(contenu_fichier))

        # Convertir la colonne `date` en type datetime avec gestion des erreurs
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y', errors='coerce')

        # Schéma de validation
        schema = {
            'transaction_id': {'type': int, 'required': True, 'unique': True, 'nullable': False},
            'product_name': {'type': str, 'nullable': False},
            'category': {'type': str, 'nullable': False},
            'price': {'type': float, 'nullable': False},
            'quantity': {'type': int, 'nullable': False},
            'date': {'type': pd.Timestamp, 'nullable': False},
            'customer_name': {'type': str, 'nullable': True},
            'customer_email': {'type': str, 'nullable': True}
        }

        # Validation des données
        erreurs = []
        for indice, ligne in df.iterrows():
            for colonne, regles in schema.items():
                # ... (vos autres vérifications) ...

                # Vérification des valeurs négatives
                if colonne in ['transaction_id', 'price', 'quantity'] and not pd.isna(ligne[colonne]):
                    try:
                        # Convertir la valeur en numérique avant la comparaison
                        if isinstance(ligne[colonne], int):
                            valeur = ligne[colonne]
                        else:
                            valeur = float(ligne[colonne])

                        if valeur < 0:
                            erreurs.append(f"Valeur négative non autorisée pour {colonne} à la ligne {indice + 1}")
                    except ValueError:
                        # Gérer le cas où la conversion échoue (la valeur n'est pas numérique)
                        erreurs.append(f"Valeur non numérique pour {colonne} à la ligne {indice + 1}")

        # Affichage des erreurs
        if erreurs:
            for erreur in erreurs:
                print(erreur)
        else:
            print("Validation réussie !")

        # Déplacer les fichiers dans les dossiers appropriés
        if erreurs:
            # Fichiers avec erreurs -> /test_error/
            print(f"{len(erreurs)} erreurs détectées. Déplacement du fichier dans '/test_error/'.")
            nom_blob_destination = fichier_csv.name.replace("input/", "error/")
        else:
            # Fichiers sans erreurs -> /test_clean/
            print("Aucune erreur détectée. Déplacement du fichier dans '/clean/'.")
            nom_blob_destination = fichier_csv.name.replace("input/", "clean/")

        # Copier le fichier vers le nouvel emplacement
        nouveau_blob = bucket.blob(nom_blob_destination)
        nouveau_blob.rewrite(fichier_csv)

        # Supprimer l'ancien fichier
        fichier_csv.delete()
        print(f"Fichier déplacé vers {nom_blob_destination} \n")

# Liste des fichiers dans le dossier 'test_clean/'
fichiers_csv = bucket.list_blobs(prefix='clean/')

for fichier in fichiers_csv:
    if fichier.name.endswith('.csv'):  # Filtrer uniquement les fichiers CSV
        print(f"Traitement du fichier : {fichier.name}")

        # Lecture du fichier CSV depuis le bucket
        contenu = fichier.download_as_bytes()
        df = pd.read_csv(BytesIO(contenu))

        # Convertir la colonne `date` en type datetime avec gestion des erreurs
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y', errors='coerce')

        # Insertion des données dans BigQuery
        try:
            parametres_insertion = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("transaction_id", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                    bigquery.SchemaField("customer_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("customer_email", "STRING", mode="NULLABLE")
                ],
                write_disposition="WRITE_APPEND",  # Ajouter les données à la table existante
                # Ignorer la première ligne (en-tête)
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV 
            )

            # Conversion du DataFrame en CSV pour l'insertion
            csv_data = df.to_csv(index=False) 

            # Charger les données depuis une chaîne de caractères CSV
            job = client_bigquery.load_table_from_file(
                BytesIO(csv_data.encode()), nom_table, job_config=job_config
            )
            job.result()  # Attendre que l'insertion soit terminée

            print(f"Données du fichier {fichier_csv.name} insérées dans la table {nom_table}")

            # Déplacer le fichier vers le dossier /done/
            nom_blob_destination = fichier.name.replace("clean/", "done/")
            nouveau_blob = bucket.blob(nom_blob_destination)
            nouveau_blob.rewrite(fichier)
            fichier.delete()
            print(f"Fichier déplacé vers {nom_blob_destination} \n")
            print("Chargement dans la base de données 'transactions' réussie ! \n ")
            print(" ----------------- FIN DU PROGRAMME --------------- \n ")

        except Exception as e:
            print(f"Erreur lors de l'insertion des données dans BigQuery: {e}")



# In[ ]:




