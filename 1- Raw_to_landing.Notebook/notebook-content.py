# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de4d2e30-b0d9-45b4-a882-0ae2a3e543b2",
# META       "default_lakehouse_name": "Landing",
# META       "default_lakehouse_workspace_id": "51b0c4d9-b2de-4601-b821-ebed2c1aab95",
# META       "known_lakehouses": [
# META         {
# META           "id": "de4d2e30-b0d9-45b4-a882-0ae2a3e543b2"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

today_file = 'LMS_09-01-2023.csv'
processed_Date = '2026-11-23'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### READING CSV FILE

# CELL ********************

import os
from dotenv import load_dotenv
from pyspark.sql.functions import lit

# 1. Charger la clé secrète depuis lakehouse_student
load_dotenv('/lakehouse/default/Files/.env')
account_key = os.getenv('AZURE_STORAGE_KEY')

# 2. Configuration du compte et des chemins
account_name = '01msfabric30'
container_name = 'landingzone'
relative_path = 'raw'
relative_path_landing = 'landing'


# 3. Authentification Spark auprès d'Azure Data Lake Gen2
spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "SharedKey")
spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)

# 4. Construction des chemins dynamiques
adls_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{relative_path}"
landing_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{relative_path_landing}"

latest_path = f"{adls_path}/{today_file}"

# 5. Lecture et traitement du fichier CSV
df = spark.read.csv(path=latest_path, header=True, inferSchema=True)

if df.count() > 1:
    print("The file has data.")
    
    df_new = df.withColumn("Processing_Date", lit(processed_Date))
    df_new.write.format('csv') \
        .option('header', 'true') \
        .partitionBy('Processing_Date') \
        .mode('append') \
        .save(landing_path)
        
    print("Data written to landing zone successfully !")
else:
    print('This file contains only header row and no data.')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
