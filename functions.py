from datetime import datetime, timezone
from pymongo import MongoClient
import re
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()
mongo_pat = os.getenv('mongo_pat')
api_pipedrive = os.getenv('api_pipedrive')

client = MongoClient( f'mongodb+srv://{mongo_pat}',tls=True,tlsAllowInvalidCertificates=True)

def get_conso():

    result = client['legacy-api-management']['bills'].aggregate([
        {
            '$match': {
                'type': {
                    '$in': ['receipt', 'credit', 'unitary']
                },
                'createdAt': {
                    '$gte': datetime(2024, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
                }
            }
        },
        {
            '$project': {
                'billingId': 1,
                'createdAt': 1,
                'type': 1,
                'priceAmount': '$price.amount',
                'month': { '$month': '$createdAt' },
                'year': { '$year': '$createdAt' }
            }
        },
        {
            '$lookup': {
                'from': 'billings',
                'localField': 'billingId',
                'foreignField': 'id',
                'as': 'billingData'
            }
        },
        {
            '$unwind': '$billingData'
        },
        {
            '$project': {
                'billingId': 1,
                'createdAt': 1,
                'type': 1,
                'priceAmount': 1,
                'raison': '$billingData.raison',
                'companyId': '$billingData.companyId',
                'month': 1,
                'year': 1
            }
        },
        {
            '$lookup': {
                'from': 'societies',
                'let': { 'companyIdObj': { '$toObjectId': '$companyId' } },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': ['$_id', '$$companyIdObj']
                            }
                        }
                    }
                ],
                'as': 'societyData'
            }
        },
        {
            '$unwind': '$societyData'
        },
        {
            '$group': {
                '_id': {
                    'raison': '$raison',
                    'companyId': '$companyId',
                    'billingId': '$billingId',
                    'type': '$type',
                    'month': '$month',
                    'year': '$year',
                    'societyName': '$societyData.name'
                },
                'totalAmount': { '$sum': '$priceAmount' },
                'count': { '$sum': 1 }
            }
        },
        {
            '$project': {
                '_id': 0,
                'companyId': '$_id.companyId',
                'billingId': '$_id.billingId',  # Make sure billingId is projected
                'raison': '$_id.raison',
                'type': '$_id.type',
                'month': '$_id.month',
                'year': '$_id.year',
                'societyName': '$_id.societyName',
                'totalAmount': 1,
                'count': 1
            }
        },
        {
            '$sort': { 'year': 1, 'month': 1 }
        }
    ])

    # Convert the result to a DataFrame
    df = pd.DataFrame(result)

    # Replace '+Simple' with 'Plus Simple'
    df['societyName'] = df['societyName'].str.replace('+Simple', 'Plus Simple')

    # Create a new column 'year_month' by combining the 'year' and 'month' columns
    df['year_month'] = pd.to_datetime(df[['year', 'month']].assign(day=1))

    # Now group by 'societyName', 'companyId', 'raison', 'type', and 'year_month'
    df = df.groupby(['societyName', 'companyId', 'raison', 'year_month', 'month', 'year', 'billingId']).agg({
        'totalAmount': 'sum',
        'count': 'sum'
    }).reset_index()

    # Define the date ranges
    ranges = [
        (pd.Timestamp('2021-10-01'), pd.Timestamp('2022-09-30'), '2021-2022'),
        (pd.Timestamp('2022-10-01'), pd.Timestamp('2023-09-30'), '2022-2023'),
        (pd.Timestamp('2023-10-01'), pd.Timestamp('2024-09-30'), '2023-2024'),
        (pd.Timestamp('2024-10-01'), pd.Timestamp('2025-09-30'), '2024-2025')
    ]

    # Function to assign 'range' based on year_month
    def assign_range(year_month):
        for start, end, label in ranges:
            if start <= year_month <= end:
                return label
        return None

    # Apply the function to create the 'range' column
    df['range'] = df['year_month'].apply(assign_range)

    # Save the resulting DataFrame to CSV
    df.to_csv(r"C:\Users\super\PycharmProjects\data_account\csv\current.csv", index=False)

def conforme():
    # Recharger le fichier et ajouter les mois
    df = pd.read_csv(r'C:\Users\super\PycharmProjects\data_account\csv\current.csv')
    month_labels = {
        10: "01-octobre", 11: "02-novembre", 12: "03-décembre",
        1: "04-janvier", 2: "05-février", 3: "06-mars", 4: "07-avril",
        5: "08-mai", 6: "09-juin", 7: "10-juillet", 8: "11-août", 9: "12-septembre"
    }

    # Group the DataFrame and perform the aggregation
    grouped_df = df.groupby(['raison', 'year_month', 'month', 'year', 'billingId', 'range'], as_index=False).agg(
        societyName=('societyName', 'first'),  # Retain the first entry for display
        companyId=('companyId', 'first'),  # Retain the first entry for display
        totalAmount=('totalAmount', 'sum'),  # Sum of totalAmount in each group
        count=('count', 'sum')  # Sum of count in each group
    )

    # Map the month labels based on the "month" column
    grouped_df['month_labels'] = grouped_df['month'].map(month_labels)

    grouped_df.to_csv('C:/Users/super/PycharmProjects/data_account/csv/current.csv',sep = ";",index=False)

def merge_conso():
    df_conso = pd.read_csv(r'C:\Users\super\PycharmProjects\data_account\csv\current.csv', delimiter=";")
    df_since21 = pd.read_csv(r'C:\Users\super\PycharmProjects\data_account\csv\since21.csv', delimiter=";")
    merged_df = pd.concat([df_since21, df_conso]).drop_duplicates(
        subset=['raison', 'year_month', 'month', 'year', 'billingId', 'range', 'societyName', 'companyId'],
        keep='last')
    merged_df.to_csv(r"C:\Users\super\PycharmProjects\data_account\csv\conso.csv",sep = ";",index=False)

def get_base():

    result = client['legacy-api-management']['societies'].aggregate(
        [
            {
                '$match': {
                    'status': 0
                }
            }, {
            '$project': {
                'id': 1,
                'name': 1,
                'status': 1,
                'sub_price': 1,
                'fceCode': 1,
                'createdAt': 1,
                'salesName': 1,
                'fullVoucher':1,
                'pack_name': {'$arrayElemAt': ["$settings.subscriptions.name", 0]},
                'pack_price': {'$arrayElemAt': ["$settings.subscriptions.price.amount", 0]},
                'bluebizz': '$settings.flight.bluebizz',
                'ssoConnect': '$settings.config.ssoConnect'
            }
        }, {
            '$lookup': {
                'from': 'items',
                'localField': 'id',
                'foreignField': 'society._id',
                'as': 'items'
            }
        }, {
            '$project': {
                'id': 1,
                'name': 1,
                'status': 1,
                'sub_price': 1,
                "pack_name":1,
                "pack_price":1,
                 'fceCode': 1,
                'createdAt': 1,
                'bluebizz': 1,
                'ssoConnect': 1,
                'salesName':1,
                'fullVoucher': 1,

            }
        }, {
            '$sort': {
                'createdAt': -1
            }
        }
        ])

    # Convertir le résultat en DataFrame
    df = pd.DataFrame(result)

    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)
    df.to_csv('C:/Users/super/PycharmProjects/data_account/csv/base.csv', index=False)

def get_portefeuille():

    FILTER_ID = 1514
    url = f"https://api.pipedrive.com/v1/organizations?filter_id={FILTER_ID}&limit=500&api_token={api_pipedrive}"

    headers = {'Accept': 'application/json'}

    response = requests.get(url, headers=headers).json()['data']

    status_mapping = {
        "763": "ACTIF", "755": "ACTIF", "746": "ACTIF",
        "747": "INACTIF", "749": "TEST", "750": "TEST",
        "748": "INACTIF", "751": "INACTIF"
    }

    data = []
    for org in response:

        society_id = org['9d0760fac9b60ea2d3f590d3146d758735f2896d']
        society_name = org['name']
        actif = status_mapping.get(org['a056613671b057f83980e4fd4bb6003ce511ca3d'],
                                     org['a056613671b057f83980e4fd4bb6003ce511ca3d'])
        golive = str(org['24582ea974bfcb46c1985c3350d33acab5e54246'])[:10]
        signature = org['af6c7d5ca6bec13a3a2ac0ffe4f05ed98907c412']
        awarde = org['446585f9020fe3190ca0fa5ef53fc429ef4b4441']
        churn = org['eda2124e4e8bed55f7f2642cf3b5238d4bfccd58']
        fin_contrat = org['7381f1cd157f298aaf3b74f90f23cdb8a7cacda3']
        account_info = org.get('e058ea93145bdf66d23b89dfab0d8f74178bb23b', {})
        account_name = account_info.get('name') if account_info else None

        data.append({
            'societyName': society_name,
            'societyId': society_id,
            'company_status': actif,
            'company_golive': golive,
            'signature': signature,
            'fin_contrat': fin_contrat,
            'awarde': awarde,
            'account': account_name,
            'churn': churn
        })

    df = pd.DataFrame(data)
    df = df.astype(str)
    df.to_csv("C:/Users/super/PycharmProjects/data_account/csv/pipe_all.csv", index=False)

def get_tarif():
    # Pipeline d'agrégation
    result = client['legacy-api-management']['societies'].aggregate([
        {
            '$match': {
                'status': 0,
                'name': {
                    '$not': re.compile(r"(?i)newrest")
                }
            }
        },
        {
            '$project': {
                'name': 1,
                'hotel': '$settings.hotel.SAB',
                'flights': '$settings.flight.sabre.vendor',
                'corporateCodes': '$corporateCodes',
                'full': '$fullVoucher',
                'fareCodeValue': {
                    '$arrayElemAt': [
                        {
                            '$map': {
                                'input': {
                                    '$filter': {
                                        'input': "$settings.flight.sabre",
                                        'as': "item",
                                        'cond': { '$eq': ["$$item.type", "FareCode"] }
                                    }
                                },
                                'as': "filteredItem",
                                'in': "$$filteredItem.value"
                            }
                        },
                        0
                    ]
                }
            }
        }
    ])

    # Convertir le résultat en DataFrame
    df = pd.DataFrame(list(result))

    # Remplacer '+Simple' par 'Plus Simple' dans la colonne 'name'
    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)

    # Convertir les colonnes en chaînes de caractères, supprimer les crochets et remplacer NaN par des chaînes vides
    df['corporateCodes'] = df['corporateCodes'].astype(str).str.replace(r"^\{|\}$", "", regex=True).replace("'", "", regex=True)
    df['flights'] = df['flights'].astype(str).str.replace(r"^\[|\]$", "", regex=True).replace("'", "", regex=True)

    df = df.fillna("").astype(str)
    df['societyName'] = df['societyName'].str.upper()
    df.to_csv('test.csv')

    base_df = pd.read_csv('C:/Users/super/PycharmProjects/data_account/csv/conso.csv', delimiter=";")
    base_df = base_df.rename(columns={'companyId':"societyId"})

    base_df['societyName'] = base_df['societyName'].str.upper()

    # # Fusionner avec base_df
    merged_df = pd.merge(df, base_df, on=["societyId", 'societyName'], how="left")

    pipe_all_df = pd.read_csv('C:/Users/super/PycharmProjects/data_account/csv/pipe_all.csv')
    pipe_all_df['societyName'] = pipe_all_df['societyName'].str.upper()

    final_df = pd.merge(merged_df, pipe_all_df, on=["societyId"], how="left")

    final_df.rename(columns={'societyName_x': 'societyName'}, inplace=True)
    final_df['full'] = final_df['full'].replace('True', "Plein Credit").replace('', "-")
    final_df['company_status'] = final_df['company_status'].fillna("-").astype(str)
    final_df.to_csv('C:/Users/super/PycharmProjects/data_account/csv/tarif.csv', index=False)

def get_entities():

    # Agrégation initiale pour extraire les entités
    result = client['legacy-api-management']['societies'].aggregate(
        [
            {
                '$unwind': '$billings'
            }, {
            '$project': {
                'name': 1,
                '_id': 1,
                "raison": "$billings.raison",
                "address": "$billings.address.label",
                "service": "$billings.service",
                "mandatId": "$billings.mandatId",
                "mandat_status": "$billings.status",
                "amex": "$settings.amex.cardHolderName",
                "billing_id": "$billings._id"  # Ajout de billings._id pour la recherche ultérieure
            }
        }
        ]
    )

    # Convertir le résultat en DataFrame
    df = pd.DataFrame(list(result))

    # Remplacer '+Simple' par 'Plus Simple' dans la colonne 'name'
    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)

    # Fonction pour créer la colonne "payment"
    def remplir_payment(row):
        if pd.notna(row['mandatId']) and pd.isna(row['amex']):
            return "mandat"
        elif pd.notna(row['amex']):
            return "carte logée"
        else:
            return "virement"

    # Appliquer la fonction à chaque ligne pour la colonne "payment"
    df['payment'] = df.apply(remplir_payment, axis=1)

    # Supprimer la colonne 'billing_id' si elle n'est plus nécessaire
    df.drop(columns=['billing_id'], inplace=True)

    # Sauvegarder le DataFrame en CSV
    df.to_csv('C:/Users/super/PycharmProjects/data_account/csv/entities.csv', index=False)

    print("CSV saved successfully.")

def get_entities_unactive():

    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from pymongo import MongoClient
    from bson import ObjectId

    # Pipeline d'agrégation pour récupérer les données de MongoDB
    pipeline = [
        {
            '$match': {
                'status': 'unactive'
            }
        },
        {
            '$addFields': {
                'companyIdAsObjectId': {'$toObjectId': '$companyId'}
            }
        },
        {
            '$lookup': {
                'from': 'societies',
                'localField': 'companyIdAsObjectId',
                'foreignField': '_id',
                'as': 'company_info'
            }
        },
        {
            '$unwind': '$company_info'
        },
        {
            '$project': {
                '_id': 1,  # Inclure _id
                'name': '$company_info.name',  # Inclure name de la collection societies
                'raison': 1,  # Inclure raison
                'address': '$address.label',  # Inclure adresse
                'status': 1,


                # Inclure status


            }
        }
    ]

    # Exécution de l'agrégation
    results = list(client['legacy-api-management']['billings'].aggregate(pipeline))

    # Transformation des résultats en DataFrame pandas
    df = pd.DataFrame(results)

    # Conversion des ObjectId en chaînes de caractères
    df['_id'] = df['_id'].astype(str)

    # Réorganisation des colonnes dans l'ordre souhaité
    df = df[['_id', 'name', 'raison', 'address', 'status']]

    # ID de votre feuille Google Sheets
    SPREADSHEET_ID = '1TI28QrhQ63i2bYgbOj4QFdriecANSalxXCZx3LEPCr8'

    # Onglet pour les entités inactives
    SHEET_NAME_UNACTIVE = 'Entities_Unactive'

    # Onglet pour les entités actives (nouvel onglet)
    SHEET_NAME_ACTIVE = 'Entities_Active'

    # Authentification Google Sheets
    SERVICE_ACCOUNT_FILE = 'C:/Users/super/PycharmProjects/data_account/creds/n8n-api-311609-115ae3a49fd9.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    # Convertir le DataFrame des entités inactives en une liste de listes
    values_unactive = df.values.tolist()
    values_unactive.insert(0, df.columns.tolist())  # Ajouter les noms de colonnes en haut

    # Préparation des données pour les entités inactives
    body_unactive = {
        'values': values_unactive
    }

    # Mise à jour de l'onglet des entités inactives
    result_unactive = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME_UNACTIVE,
        valueInputOption='RAW',
        body=body_unactive
    ).execute()

    print(f"{result_unactive.get('updatedCells')} cellules mises à jour dans '{SHEET_NAME_UNACTIVE}'.")

    # Charger le fichier CSV entities.csv dans un DataFrame
    df_entity = pd.read_csv('C:/Users/super/PycharmProjects/data_account/csv/entities.csv')

    # Nettoyer les données en remplaçant les NaN par une chaîne vide
    df_entity = df_entity.fillna('')

    # Convertir le DataFrame en une liste de listes
    values_active = df_entity.values.tolist()
    values_active.insert(0, df_entity.columns.tolist())  # Ajouter les noms de colonnes en haut

    # Préparation des données pour les entités actives
    body_active = {
        'values': values_active
    }

    # Mise à jour de l'onglet pour les entités actives
    result_active = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME_ACTIVE,  # Écrire dans un autre onglet
        valueInputOption='RAW',
        body=body_active
    ).execute()

    print(f"{result_active.get('updatedCells')} cellules mises à jour dans '{SHEET_NAME_ACTIVE}'.")

def merge_all():
    # Load the three CSV files
    conso_df = pd.read_csv('C:/Users/super/PycharmProjects/data_account/csv/conso.csv', delimiter = ";",encoding='utf-8')
    base_df = pd.read_csv('C:/Users/super/PycharmProjects/data_account/csv/base.csv')
    pipe_all_df = pd.read_csv('C:/Users/super/PycharmProjects/data_account/csv/pipe_all.csv')
    print(conso_df)

    conso_df = conso_df.rename(columns={'companyId': 'societyId'})
    # Merge the three DataFrames on 'societyId', giving priority to 'conso'
    merged_df = conso_df.merge(base_df, on='societyId', how='left').merge(pipe_all_df, on='societyId', how='left')

    # Drop duplicate columns if any (keeping the first occurrence)
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    merged_df = merged_df.rename(columns={'societyName_x': 'societyName'})
    # merged_df = merged_df[merged_df['company_status'] == 'ACTIF'].reset_index()

    # Save the merged result to a new CSV file
    merged_df.to_csv('C:/Users/super/PycharmProjects/data_account/csv/merged_result.csv',index=False)

    # Display the first few rows of the merged DataFrame
    merged_df.head()

def update_drive():

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    # Liste des tuples contenant les paires de (nom_fichier_csv, plage_sheet)
    list = [('merged_result.csv', 'conso_updated'),('tarif.csv', 'tarif_nego'),('warning.csv', 'warning'),('entities.csv', 'entities'),('entities_unactive.csv', 'entities_unactive')]

    # Fichier de compte de service et les scopes de l'API
    SERVICE_ACCOUNT_FILE = 'C:/Users/super/PycharmProjects/data_account/creds/n8n-api-311609-115ae3a49fd9.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def sheet_upt(filename, sheet_range):
        # ID de votre feuille Google Sheets
        SPREADSHEET_ID = '17VFyCP-CKmNl1X1BRVdc0jNjC4o9hyAopRrb5fITAK8'

        # Authentification et construction du service Google Sheets
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)

        # Lecture du DataFrame pivoté
        df = pd.read_csv(f'C:/Users/super/PycharmProjects/data_account/csv/{filename}', low_memory=False)
        df = df.astype(str).replace('nan', '')
        # Convertir la colonne "totalAmount" en float
        if 'totalAmount' in df.columns:
            df['totalAmount'] = pd.to_numeric(df['totalAmount'], errors='coerce')

        if 'sub_price' in df.columns:
            df['sub_price'] = pd.to_numeric(df['sub_price'], errors='coerce')

        if '2022-2023' in df.columns:
            df['2022-2023'] = pd.to_numeric(df['2022-2023'], errors='coerce')

        if '2023-2024' in df.columns:
            df['2023-2024'] = pd.to_numeric(df['2023-2024'], errors='coerce')

        if '2024-2025' in df.columns:
            df['2024-2025'] = pd.to_numeric(df['2024-2025'], errors='coerce')

        # Remplacer les NaN par des chaînes vides pour éviter des erreurs dans l'API Google Sheets
        df = df.fillna("")

        df = df[df.columns.drop(df.filter(regex='_x$|_y$').columns)]

        # Convertir le DataFrame en une liste de listes (comme attendu par l'API Google Sheets)
        values = df.values.tolist()
        values.insert(0, df.columns.tolist())  # Ajouter les noms de colonnes en haut

        # Préparation des données pour l'API
        body = {
            'values': values
        }

        # Mise à jour de la feuille Google Sheets
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_range,
            valueInputOption='RAW',
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} cellules mises à jour pour {filename} vers {sheet_range}.")

    # Boucle sur la liste des fichiers et plages
    for filename, sheet_range in list:
        sheet_upt(filename, sheet_range)

def envoi_email(status,error):
    SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']

    sender_email = 'ope@supertripper.com'
    sender_name = 'Supertripper Reports'
    recipient_email = "ope@supertripper.com"
    subject = f'CONSO BILLS : CRON {status}'

    # Construction du corps de l'e-mail
    body = (
        f'{error}'
    )
    creds_file = 'C:/Users/super/PycharmProjects/data_account/creds/cred_gmail.json'
    token_file = 'C:/Users/super/PycharmProjects/data_account/token.json'
    def authenticate_gmail():
        """Authentifie l'utilisateur via OAuth 2.0 et retourne les credentials"""
        creds = None
        # Le token est stocké localement après la première authentification
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        # Si le token n'existe pas ou est expiré, on initie un nouveau flux OAuth
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Enregistrer le token pour des sessions futures
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    def create_message_with_attachment(sender, sender_name, to, subject, message_text):
        """Crée un e-mail avec une pièce jointe et un champ Cc"""
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = f'{sender_name} <{sender}>'
        message['subject'] = subject

        # Attacher le corps du texte
        message.attach(MIMEText(message_text, 'plain'))

        # Encoder le message en base64 pour l'envoi via l'API Gmail
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return {'raw': raw_message}

    def send_email(service, user_id, message):
        """Envoie un e-mail via l'API Gmail"""
        try:
            message = service.users().messages().send(userId=user_id, body=message).execute()
            print(f"Message Id: {message['id']}")
            return message
        except HttpError as error:
            print(f'An error occurred: {error}')
            return None

    # Authentifier l'utilisateur et créer un service Gmail
    creds = authenticate_gmail()
    service = build('gmail', 'v1', credentials=creds)

    # Créer le message avec pièce jointe et copie
    message = create_message_with_attachment(sender_email, sender_name, recipient_email, subject, body)

    # Envoyer l'e-mail
    send_email(service, 'me', message)
    print("Mail envoyé pour vérif ")

