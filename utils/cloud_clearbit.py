import clearbit
import psycopg2
import os
import snowflake.connector
import sys
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
from extract.utils import snowflake_engine_factory, execute_query, execute_dataframe

# Create database connection and cursor
engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
connection = engine.connect()

# SET CLEARBIT API KEY
clearbit.key = os.getenv('CLEARBIT_KEY')

# DETERMINE CLOUD_CLEARBIT TABLE IS BUILT AND HAS NOT BEEN DROPPED
try:
    query = f'''SELECT * FROM ANALYTICS.MATTERMOST.CLOUD_CLEARBIT'''
    test = execute_dataframe(engine, query)
except:
    test = None

# RETRIEVE ALL WORKSPACES THAT HAVE NOT ALREADY BEEN ENRICHED BY CLEARBIT
q = f'''
SELECT 
    LSF.LICENSE_EMAIL
  , SPLIT_PART(LICENSE_EMAIL, '@', 2) AS EMAIL_DOMAIN
  , SF.SERVER_ID
  , SF.INSTALLATION_ID
  , COALESCE(SF.FIRST_ACTIVE_DATE, CURRENT_DATE) AS FIRST_ACTIVE_DATE
FROM ANALYTICS.MATTERMOST.SERVER_FACT SF
JOIN ANALYTICS.BLP.LICENSE_SERVER_FACT LSF
    ON SF.SERVER_ID = LSF.SERVER_ID
LEFT JOIN ANALYTICS.MATTERMOST.EXCLUDABLE_SERVERS ES
    ON SF.SERVER_ID = ES.SERVER_ID
{'LEFT JOIN ANALYTICS.MATTERMOST.CLOUD_CLEARBIT CB ON SF.SERVER_ID = CB.SERVER_ID' if test is not None else ''}
WHERE SF.INSTALLATION_ID IS NOT NULL
AND ES.REASON IS NULL
{'AND CB.SERVER_ID IS NULL' if test is not None else ''}
ORDER BY COALESCE(SF.FIRST_ACTIVE_DATE, CURRENT_DATE) ASC
'''
df = execute_dataframe(engine, query=q)

# RETRIEVE CLEARBIT DATA FROM API USING CLEARBIT.ENRICHMENT.FIND AND THE USER'S EMAIL ADDRESS THAT CREATED THE CLOUD WORKSPACE
cloud_clearbit = []
for index, row in df.iterrows():
    try:
        response = clearbit.Enrichment.find(email=f'''{row['LICENSE_EMAIL']}''', stream=True)
    except:
        respone = None
    if response is not None:
        cloud_clearbit.append([row['SERVER_ID'], response])

# CREATE EMPTY LIST TO STORE COLUMN NAMES FOR NESTED CLEARBIT DATA
cols = []

# CREATE DICTIONARY TO STORE RESULTS AS SERVER_ID:CLEARBIT RESPONSE KEY-VALUE PAIRS
d = {}

# RETRIEVE ALL COLUMN NAMES REQUIRED TO GENERATE A DATAFRAME TO STORE EACH CLEARBIT PROPERTY AND STORE IN LIST
for i in cloud_clearbit:
    for key, value in i[1].items():
        if isinstance(value,dict):
            for k, v in value.items():
                if isinstance(v, dict):
                    for k1, v1 in v.items():
                        if isinstance(v1, dict):
                            for k2, v2 in v1.items():
                                cols.append(key + '_' + k + '_' + k1 + '_' + k2)
                        else:
                            cols.append(key + '_' + k + '_' + k1)
                else:
                    cols.append(key + '_' + k)
        else:
            cols.append(key)
# GENERATE SERVER_ID:CLEARBIT RESPONSE KEY-VALUE PAIR RECORDS AND STORE IN DICTIONARY
    d[i[0]] = i[1]
# ONLY RETAIN UNIQUE COLUMN VALUES AS SET THEN RECAST AS LIST FOR ITERATING PURPOSES.
cols = set(cols)
cols = list(cols)

# CREATE CLEARBIT DATAFRAME STRUCTURE USING UNIQUE COLUMN VALUES
clearbit_df = pd.DataFrame(columns=cols)

# ADD A ROW FOR EACH UNIQUE WORKSPACE (SERVER_ID)
clearbit_df['server_id'] = df['SERVER_ID'].unique()

# ITERATE THROUGH CLOUD WORKSPACE CLEARBIT KEY-VALUE PAIRS
# UPDATE EACH RESPECTIVE COLUMN PROPERTY USING INDEX AND COLUMN NAME
for key, value in d.items():
    for index, row in clearbit_df[clearbit_df['server_id'] == key].iterrows():
        for k, v in value.items():
            if isinstance(v,dict):
                for k1, v1 in v.items():
                    if isinstance(v1, dict):
                        for k2, v2 in v1.items():
                            if isinstance(v2, dict):
                                for k3, v3 in v2.items():
                                    clearbit_df.loc[index, k + '_' + k1 + '_' + k2 + '_' + k3] = v3
                            else:
                                clearbit_df.loc[index, k + '_' + k1 + '_' + k2] = v2
                    else:
                        clearbit_df.loc[index, k + '_' + k1] = v1
            else:
                clearbit_df.loc[index, k] = v

# Convert clearbit object data types to next best fit.
clearbit_df2 = clearbit_df.convert_dtypes()

# CAST REMAINING CLEARBIT OBJECT COLUMNS TO STRINGS
columns = ['company_site_phoneNumbers','company_techCategories','company_domainAliases','company_tech','person_gravatar_urls','person_gravatar_avatars',\
          'company_tags','company_site_emailAddresses']
clearbit_df2[columns] = clearbit_df2[columns].astype(str)

# CONVERT COLUMN NAMES TO LOWERCASE FOR LOADING PURPOSES
clearbit_df2.columns = clearbit_df2.columns.str.lower()

# ADD NEW WORKSPACE ROWS TO CLOUD_CLEARBIT TABLE
clearbit_df2.to_sql("cloud_clearbit", con=connection, index=False, schema="MATTERMOST", if_exists="append")



