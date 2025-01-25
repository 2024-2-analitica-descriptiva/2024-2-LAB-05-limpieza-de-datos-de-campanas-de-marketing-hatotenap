"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
from datetime import datetime

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    # Definimos las carpetas de entrada y salida
    input_folder = "files/input/"
    output_folder = "files/output/"
    # Se crea la carpeta de salida si no existe para evitar errores al guardar los archivos.
    os.makedirs(output_folder, exist_ok=True)

    # vamos a identificar los archivos comprimidos en formato .csv.zip, estos se encuentran en la carpeta input_folder
    # os.listdir devuelve un alista con los nombres de todos los archivos dentro de la ruta input_folder ejm: (['bank-data.csv.zip', 'summary.pdf', 'report.csv.zip'])
    # con f.endswith('.csv.zip): solo los nombres de archivo que terminan con .csv.zip serán incluidos en la lista files
    files = [f for f in os.listdir(input_folder) if f.endswith('.csv.zip')]

    # Almacenes para cada categoría
    # se crean listas vacías donde se almacenarán temporalmente los datos limpios antes de guardarlos en archivos
    client_data = []
    campaign_data = []
    economics_data = []

    # Realizamos el procesamiento de cada archivo comprimido
    # iteramos sobre cada archivo .zip en la carpeta files
    for file_name in files:
        # con zipfile.ZipFile se abre el archivo .zip para lectura ('r')
        with zipfile.ZipFile(os.path.join(input_folder, file_name), 'r') as z:
            #z.namelost(): Devuelve una lsita con los nombres de todos los archivos contenidos en el ZIP
            for csv_file in z.namelist():
                with z.open(csv_file) as f:
                    # Leer cada CSV dentro del ZIP
                    df = pd.read_csv(f)

                    # Limpieza de datos
                    # Client data
                    #Seleccionamos las columnas más relevantes para el archivo client.csv
                    client = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
                    client['job'] = client['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
                    # para education remplaza los puntos por guines bajos 
                    client['education'] = client['education'].str.replace('.', '_', regex=False)
                      # .replace('unknown', pd.NA):cambia los valores 'unknow' por pd.NA que representa valores faltantes en pandas.
                    client['education'] = client['education'].replace('unknown', pd.NA)
                    # convertimos todos los 1 a yes y el resto a 0
                    client['credit_default'] = client['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
                    client['mortgage'] = client['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
                    client_data.append(client)

                    # Campaign data
                    campaign = df[['client_id', 'number_contacts', 'contact_duration',
                                   'previous_campaign_contacts', 'previous_outcome',
                                   'campaign_outcome', 'day', 'month']].copy()
                    campaign['previous_outcome'] = campaign['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
                    campaign['campaign_outcome'] = campaign['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
                    # combinamos las columnas day y month con el año 2022 para generar una nueva columan llamada last_contact_date con el formato yyyy-mm-dd
                    campaign['last_contact_date'] = pd.to_datetime(
                        #pasamos las columnas day y month a string  y los unimos 
                        campaign['day'].astype(str) + '-' + campaign['month'].astype(str) + '-2022',
                        # convierte la cadaena format '%d-%b-%Y en una fecha  (ejm: 19 jul 2022)
                        format='%d-%b-%Y',
                        # convierte los valores inválidos en NaT (Not a Time)
                        errors='coerce'
                        # Con dr.strftime convierte la fecha al formato YYY-mm-dd
                    ).dt.strftime('%Y-%m-%d')
                    # Elimnar las columnas day y month del dataframe porque ya no son necesarias después de crear last_contact_date
                    campaign = campaign.drop(columns=['day', 'month'])
                    # guarda la data y aprocesada a la lista vacía creada en el princio
                    campaign_data.append(campaign)

                    # Economics data
                    economics = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
                    economics_data.append(economics)

    # Rutas de los archivos generados
    client_file = os.path.join(output_folder, 'client.csv')
    campaign_file = os.path.join(output_folder, 'campaign.csv')
    economics_file = os.path.join(output_folder, 'economics.csv')

    # Guardar los archivos CSV
    pd.concat(client_data, ignore_index=True).to_csv(client_file, index=False)
    pd.concat(campaign_data, ignore_index=True).to_csv(campaign_file, index=False)
    pd.concat(economics_data, ignore_index=True).to_csv(economics_file, index=False)

    # Retornar las rutas de los archivos generados
    return [client_file, campaign_file, economics_file]



if __name__ == "__main__":
    clean_campaign_data()
