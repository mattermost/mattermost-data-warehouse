import os

import clearbit
import pandas as pd

from extract.utils import execute_dataframe, snowflake_engine_factory


def cloud_clearbit():
    # Create database connection and cursor
    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
    with engine.connect() as connection:

        # SET CLEARBIT API KEY
        clearbit.key = os.getenv('CLEARBIT_KEY')

        # RETRIEVE COLUMN NAMES IF TABLE ALREADY EXISTS
        try:
            col_q = '''
            SELECT COLUMN_NAME
            FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'CLOUD_CLEARBIT'
            AND TABLE_SCHEMA = 'MATTERMOST'
            ORDER BY ORDINAL_POSITION
            '''
            col_df = execute_dataframe(engine, query=col_q)
            clearbit_cols = []
            if len(col_df) > 0:
                for _index, row in col_df.iterrows():
                    clearbit_cols.append(row['COLUMN_NAME'].lower())
            else:
                clearbit_cols = None

        except Exception:
            clearbit_cols = None

        # DETERMINE CLOUD_CLEARBIT TABLE IS BUILT AND HAS NOT BEEN DROPPED
        try:
            query = '''SELECT * FROM ANALYTICS.MATTERMOST.CLOUD_CLEARBIT LIMIT 10'''
            test = execute_dataframe(engine, query=query)
        except Exception:
            test = None

        # RETRIEVE ALL WORKSPACES THAT HAVE NOT ALREADY BEEN ENRICHED BY CLEARBIT
        q = f'''
        SELECT
            LSF.LICENSE_EMAIL
          , SPLIT_PART(LICENSE_EMAIL, '@', 2) AS EMAIL_DOMAIN
          , SF.SERVER_ID
          , SF.INSTALLATION_ID
          , COALESCE(SF.FIRST_ACTIVE_DATE, CURRENT_DATE) AS FIRST_ACTIVE_DATE
          , SF.LAST_IP_ADDRESS
        FROM ANALYTICS.MATTERMOST.SERVER_FACT SF
        JOIN ANALYTICS.BLP.LICENSE_SERVER_FACT LSF
            ON SF.SERVER_ID = LSF.SERVER_ID
        LEFT JOIN ANALYTICS.MATTERMOST.EXCLUDABLE_SERVERS ES
            ON SF.SERVER_ID = ES.SERVER_ID
        {'LEFT JOIN ANALYTICS.MATTERMOST.CLOUD_CLEARBIT CB ON SF.SERVER_ID = CB.SERVER_ID' if test is not None else ''}
        WHERE ES.REASON IS NULL
        {'AND CB.SERVER_ID IS NULL' if test is not None else ''}
        AND SF.FIRST_ACTIVE_DATE::DATE >= '2020-02-01'
        AND SF.INSTALLATION_ID IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY COALESCE(SF.FIRST_ACTIVE_DATE, CURRENT_DATE) DESC
        '''
        df = execute_dataframe(engine, query=q)

        # RETRIEVE CLEARBIT DATA FROM API USING CLEARBIT.ENRICHMENT.FIND AND THE USER'S EMAIL ADDRESS THAT CREATED THE
        # CLOUD WORKSPACE
        cloud_clearbit = []
        exceptions = 0
        cloud_exceptions = []
        response = None
        for _index, row in df.iterrows():
            response = None
            try:
                response = clearbit.Enrichment.find(email=f'''{row['LICENSE_EMAIL']}''', stream=True)
            except Exception:
                response = None

            if response is not None:
                cloud_clearbit.append([row['SERVER_ID'], response])
                response = None
            else:
                exceptions += 1
                cloud_exceptions.append([row['SERVER_ID']])

        # CHECK IF NEW DATA TO LOAD
        if len(cloud_clearbit) >= 1:
            # USE EXISTING COLUMN NAMES IF TABLE ALREADY EXISTS
            if clearbit_cols is not None:
                d = {}
                clearbit_df = pd.DataFrame(columns=clearbit_cols)
                clearbit_df['server_id'] = df[df['INSTALLATION_ID'].notnull()]['SERVER_ID'].unique()
                for i in cloud_clearbit:
                    d[i[0]] = i[1]
            else:
                # CREATE EMPTY LIST TO STORE COLUMN NAMES FOR NESTED CLEARBIT DATA
                cols = []
                # CREATE DICTIONARY TO STORE RESULTS AS SERVER_ID:CLEARBIT RESPONSE KEY-VALUE PAIRS
                d = {}
                # RETRIEVE ALL COLUMN NAMES REQUIRED TO GENERATE A DATAFRAME TO STORE EACH CLEARBIT PROPERTY AND STORE
                # IN LIST
                for i in cloud_clearbit:
                    for key, value in i[1].items():
                        if isinstance(value, dict):
                            for k, v in value.items():
                                if isinstance(v, dict):
                                    for k1, v1 in v.items():
                                        if isinstance(v1, dict):
                                            for k2, _v2 in v1.items():
                                                cols.append(
                                                    key.lower() + '_' + k.lower() + '_' + k1.lower() + '_' + k2.lower()
                                                )
                                        else:
                                            cols.append(key.lower() + '_' + k.lower() + '_' + k1.lower())
                                else:
                                    cols.append(key.lower() + '_' + k.lower())
                        else:
                            cols.append(key.lower())
                    # GENERATE SERVER_ID:CLEARBIT RESPONSE KEY-VALUE PAIR RECORDS AND STORE IN DICTIONARY
                    d[i[0]] = i[1]
                # ONLY RETAIN UNIQUE COLUMN VALUES AS SET THEN RECAST AS LIST FOR ITERATING PURPOSES.
                cols = set(cols)
                cols = list(cols)

                # CREATE CLEARBIT DATAFRAME STRUCTURE USING UNIQUE COLUMN VALUES
                clearbit_df = pd.DataFrame(columns=cols)

                # ADD A ROW FOR EACH UNIQUE WORKSPACE (SERVER_ID)
                clearbit_df['server_id'] = df[df['INSTALLATION_ID'].notnull()]['SERVER_ID'].unique()

            # ITERATE THROUGH CLOUD WORKSPACE CLEARBIT KEY-VALUE PAIRS
            # UPDATE EACH RESPECTIVE COLUMN PROPERTY USING INDEX AND COLUMN NAME
            for key, value in d.items():
                for index, _row in clearbit_df[clearbit_df['server_id'] == key].iterrows():
                    for k, v in value.items():
                        if isinstance(v, dict):
                            for k1, v1 in v.items():
                                if isinstance(v1, dict):
                                    for k2, v2 in v1.items():
                                        if isinstance(v2, dict):
                                            for k3, v3 in v2.items():
                                                clearbit_df.loc[
                                                    index,
                                                    k.lower() + '_' + k1.lower() + '_' + k2.lower() + '_' + k3.lower(),
                                                ] = v3
                                                if isinstance(v3, dict):
                                                    for k4, v4 in v3.items():
                                                        clearbit_df.loc[
                                                            index,
                                                            k.lower()
                                                            + '_'
                                                            + k1.lower()
                                                            + '_'
                                                            + k2.lower()
                                                            + '_'
                                                            + k3.lower()
                                                            + '_'
                                                            + k4.lower(),
                                                        ] = v4
                                                else:
                                                    clearbit_df.loc[
                                                        index,
                                                        k.lower()
                                                        + '_'
                                                        + k1.lower()
                                                        + '_'
                                                        + k2.lower()
                                                        + '_'
                                                        + k3.lower(),
                                                    ] = v3
                                        else:
                                            clearbit_df.loc[index, k.lower() + '_' + k1.lower() + '_' + k2.lower()] = v2
                                else:
                                    clearbit_df.loc[index, k.lower() + '_' + k1.lower()] = (
                                        str(v1) if type(v1) == list and len(v1) > 0 else v1
                                    )
                        else:
                            clearbit_df.loc[index, k.lower()] = v

            # Convert clearbit object data types to next best fit.
            clearbit_df2 = clearbit_df.convert_dtypes()

            # CAST REMAINING CLEARBIT OBJECT COLUMNS TO STRINGS
            columns = [
                'company_site_phonenumbers',
                'company_techcategories',
                'company_domainaliases',
                'company_tech',
                'person_gravatar_urls',
                'person_gravatar_avatars',
                'company_tags',
                'company_site_emailaddresses',
            ]
            # Ugly fix to handle exceptions consistently with how they are currently stored in the database
            clearbit_df2[columns] = clearbit_df2[columns].fillna('nan')
            clearbit_df2[columns] = clearbit_df2[columns].astype(str)

            # CONVERT COLUMN NAMES TO LOWERCASE FOR LOADING PURPOSES
            clearbit_df2.columns = clearbit_df2.columns.str.lower()

            # If the list of columns is known (table already exists), then don't add new columns.
            if clearbit_cols:
                clearbit_df2 = clearbit_df2[clearbit_cols]

            engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
            with engine.connect() as connection:

                # ADD NEW WORKSPACE ROWS TO CLOUD_CLEARBIT TABLE
                clearbit_df2.to_sql(
                    "cloud_clearbit",
                    con=connection,
                    index=False,
                    schema="MATTERMOST",
                    if_exists="append",
                    chunksize=5000,
                )
                print(f'''Success. Uploaded {len(clearbit_df2)} rows to ANALYTICS.MATTERMOST.CLOUD_CLEARBIT''')
                print(f'''Exceptions: {exceptions}''')
                print(f'''Exception Server ID's: {cloud_exceptions}''')
        else:
            print("Nothing to do.")


if __name__ == '__main__':
    cloud_clearbit()
