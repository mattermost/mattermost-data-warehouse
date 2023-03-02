import os

import clearbit
import pandas as pd

from extract.utils import execute_dataframe, execute_query, snowflake_engine_factory


def onprem_clearbit():
    # Create database connection and cursor
    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
    connection = engine.connect()

    # SET CLEARBIT API KEY
    clearbit.key = os.getenv('CLEARBIT_KEY')

    # RETRIEVE COLUMN NAMES IF TABLE ALREADY EXISTS
    try:
        col_q = '''
        SELECT COLUMN_NAME
        FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'ONPREM_CLEARBIT'
        AND TABLE_SCHEMA = 'MATTERMOST'
        ORDER BY ORDINAL_POSITION
        '''
        col_df = execute_dataframe(engine, query=col_q)
        onprem_cols = []
        if len(col_df) > 0:
            for _index, row in col_df.iterrows():
                onprem_cols.append(row['COLUMN_NAME'].lower())
        else:
            onprem_cols = None

    except Exception:
        onprem_cols = None

    # DETERMINE ONPREM_CLEARBIT TABLE IS BUILT AND HAS NOT BEEN DROPPED
    try:
        query = '''SELECT * FROM ANALYTICS.MATTERMOST.ONPREM_CLEARBIT'''
        test_onprem = execute_dataframe(engine, query=query)
    except Exception:
        test_onprem = None

    try:
        query = '''SELECT * FROM ANALYTICS.STAGING.CLEARBIT_ONPREM_EXCEPTIONS'''
        exceptions_test = execute_dataframe(engine, query=query)
    except Exception:
        exceptions_test = None

    # RETRIEVE ALL SERVERS THAT HAVE NOT ALREADY BEEN ENRICHED BY CLEARBIT
    q = f'''
    SELECT
        SF.SERVER_ID
      , SF.INSTALLATION_ID
      , COALESCE(SF.FIRST_ACTIVE_DATE, CURRENT_DATE) AS FIRST_ACTIVE_DATE
      , SF.LAST_IP_ADDRESS
    FROM ANALYTICS.MATTERMOST.SERVER_FACT SF
    LEFT JOIN ANALYTICS.MATTERMOST.EXCLUDABLE_SERVERS ES
        ON SF.SERVER_ID = ES.SERVER_ID
    {
        'LEFT JOIN ANALYTICS.MATTERMOST.ONPREM_CLEARBIT OC ON SF.SERVER_ID = OC.SERVER_ID'
        if test_onprem is not None else ''
    }
    {
        'LEFT JOIN ANALYTICS.STAGING.CLEARBIT_ONPREM_EXCEPTIONS CE ON SF.SERVER_ID = CE.SERVER_ID'
        if exceptions_test is not None else ''
    }
    WHERE ES.REASON IS NULL
    {'AND OC.SERVER_ID IS NULL' if test_onprem is not None else ''}
    {'AND CE.SERVER_ID IS NULL' if exceptions_test is not None else ''}
    AND SF.FIRST_ACTIVE_DATE::DATE >= '2020-02-01'
    AND SF.INSTALLATION_ID IS NULL
    AND NULLIF(SF.LAST_IP_ADDRESS, '') IS NOT NULL
    GROUP BY 1, 2, 3, 4
    ORDER BY COALESCE(SF.FIRST_ACTIVE_DATE, CURRENT_DATE) DESC
    LIMIT 5000
    '''
    df = execute_dataframe(engine, query=q)

    # RETRIEVE CLEARBIT DATA FROM API USING CLEARBIT.REVEAL.FIND AND THE SERVER'S IP ADDRESS
    clearbit_onprem_exceptions = pd.DataFrame(columns=['SERVER_ID'])
    onprem_clearbit = []
    response_onprem = None
    for _index, row in df.iterrows():
        try:
            response_onprem = clearbit.Reveal.find(ip=row['LAST_IP_ADDRESS'])
        except Exception:
            response_onprem = None

        if response_onprem is not None:
            onprem_clearbit.append([row['SERVER_ID'], response_onprem])
            response_onprem = None
        else:
            clearbit_onprem_exceptions.append(pd.Series(row['SERVER_ID']), ignore_index=True)
            response_onprem = None

    if len(onprem_clearbit) >= 1:
        # USE EXISTING COLUMN NAMES IF TABLE ALREADY EXISTS
        if onprem_cols is not None:
            d = {}
            onprem_df = pd.DataFrame(columns=onprem_cols)
            onprem_df['server_id'] = df[df['INSTALLATION_ID'].isnull()]['SERVER_ID'].unique()
            for i in onprem_clearbit:
                d[i[0]] = i[1]
        else:
            # CREATE EMPTY LIST TO STORE COLUMN NAMES FOR NESTED CLEARBIT DATA
            cols = []
            # CREATE DICTIONARY TO STORE RESULTS AS SERVER_ID:CLEARBIT RESPONSE KEY-VALUE PAIRS
            d = {}
            # RETRIEVE ALL COLUMN NAMES REQUIRED TO GENERATE A DATAFRAME TO STORE EACH CLEARBIT PROPERTY AND STORE
            # IN LIST
            for i in onprem_clearbit:
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
            print(cols)

            # CREATE CLEARBIT DATAFRAME STRUCTURE USING UNIQUE COLUMN VALUES
            onprem_df = pd.DataFrame(columns=cols)

            # ADD A ROW FOR EACH UNIQUE WORKSPACE (SERVER_ID)
            onprem_df['server_id'] = df[df['INSTALLATION_ID'].isnull()]['SERVER_ID'].unique()

        # ITERATE THROUGH CLOUD WORKSPACE CLEARBIT KEY-VALUE PAIRS
        # UPDATE EACH RESPECTIVE COLUMN PROPERTY USING INDEX AND COLUMN NAME
        for key, value in d.items():
            for index, _row in onprem_df[onprem_df['server_id'] == key].iterrows():
                for k, v in value.items():
                    if isinstance(v, dict):
                        for k1, v1 in v.items():
                            if isinstance(v1, dict):
                                for k2, v2 in v1.items():
                                    if isinstance(v2, dict):
                                        for k3, v3 in v2.items():
                                            onprem_df.loc[
                                                index,
                                                k.lower() + '_' + k1.lower() + '_' + k2.lower() + '_' + k3.lower(),
                                            ] = v3
                                    else:
                                        onprem_df.loc[index, k.lower() + '_' + k1.lower() + '_' + k2.lower()] = v2
                            else:
                                onprem_df.loc[index, k.lower() + '_' + k1.lower()] = v1
                    else:
                        onprem_df.loc[index, k.lower()] = v

        # Convert clearbit object data types to next best fit.
        onprem_df2 = onprem_df.convert_dtypes()

        # CAST REMAINING CLEARBIT OBJECT COLUMNS TO STRINGS
        columns = [
            'company_site_phonenumbers',
            'company_techcategories',
            'company_domainaliases',
            'company_tech',
            'company_tags',
            'company_site_emailaddresses',
            'company_ultimateparent_domain',
            'company',
            'company_parent_domain',
        ]
        onprem_df2[columns] = onprem_df2[columns].astype(str)

        # CONVERT COLUMN NAMES TO LOWERCASE FOR LOADING PURPOSES
        onprem_df2.columns = onprem_df2.columns.str.lower()
        clearbit_onprem_exceptions.columns = clearbit_onprem_exceptions.columns.str.lower()

        engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
        connection = engine.connect()

        # ADD NEW WORKSPACE ROWS TO CLOUD_CLEARBIT TABLE
        onprem_df2.to_sql("onprem_clearbit", con=connection, index=False, schema="MATTERMOST", if_exists="append")
        print(f'Success. Uploaded {len(onprem_df2)} rows to ANALYTICS.MATTERMOST.ONPREM_CLEARBIT')

        clearbit_onprem_exceptions.to_sql(
            "clearbit_onprem_exceptions", con=connection, index=False, schema="STAGING", if_exists="append"
        )
        print(
            f'Success. Uploaded {len(clearbit_onprem_exceptions)} rows to ANALYTICS.STAGING.CLEARBIT_ONPREM_EXCEPTIONS'
        )

        engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
        connection = engine.connect()
        try:
            query = '''SELECT * FROM ANALYTICS.STAGING.CLEARBIT_ONPREM_EXCEPTIONS'''
            exceptions_test = execute_dataframe(engine, query=query)
        except Exception:
            exceptions_test = None

        if exceptions_test is None:
            query = '''CREATE OR REPLACE TABLE ANALYTICS.STAGING.CLEARBIT_ONPREM_EXCEPTIONS AS
                SELECT DISTINCT SERVER_ID, CURRENT_TIMESTAMP AS received_at
                FROM ANALYTICS.MATTERMOST.ONPREM_CLEARBIT
                WHERE FUZZY IS NULL;
             '''
            execute_query(engine, query)

            q2 = '''DELETE FROM ANALYTICS.MATTERMOST.ONPREM_CLEARBIT WHERE FUZZY IS NULL;'''
            execute_query(engine, query=q2)
        elif exceptions_test is not None:
            query = '''
                INSERT INTO ANALYTICS.STAGING.CLEARBIT_ONPREM_EXCEPTIONS(SERVER_ID, received_at)
                    SELECT DISTINCT SERVER_ID, CURRENT_TIMESTAMP AS received_at
                    FROM ANALYTICS.MATTERMOST.ONPREM_CLEARBIT
                    WHERE FUZZY IS NULL;
            '''
            execute_query(engine, query)

            q2 = '''DELETE FROM ANALYTICS.MATTERMOST.ONPREM_CLEARBIT WHERE FUZZY IS NULL;'''
            execute_query(engine, query=q2)
    else:
        print("Nothing to do.")


if __name__ == '__main__':
    onprem_clearbit()
