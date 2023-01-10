import logging
from typing import Any, Dict, List, Tuple

import pandas as pd
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def snowflake_engine_factory(args: Dict[str, str], role: str, schema: str = "") -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    # Figure out which vars to grab
    role_dict = {
        "SYSADMIN": {
            "USER": "SNOWFLAKE_USER",
            "PASSWORD": "SNOWFLAKE_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_LOAD_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "SYSADMIN",
        },
        "ANALYTICS_LOADER": {
            "USER": "SNOWFLAKE_LOAD_USER",
            "PASSWORD": "SNOWFLAKE_LOAD_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_TRANSFORM_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "LOADER",
        },
        "LOADER": {
            "USER": "SNOWFLAKE_LOAD_USER",
            "PASSWORD": "SNOWFLAKE_LOAD_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_LOAD_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "LOADER",
        },
        "TRANSFORMER": {
            "USER": "SNOWFLAKE_USER",
            "PASSWORD": "SNOWFLAKE_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_TRANSFORM_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_TRANSFORM_WAREHOUSE",
            "ROLE": "TRANSFORMER",
        },
        "PERMISSIONS": {
            "USER": "PERMISSION_BOT_USER",
            "PASSWORD": "PERMISSION_BOT_PASSWORD",
            "ACCOUNT": "PERMISSION_BOT_ACCOUNT",
            "DATABASE": "PERMISSION_BOT_DATABASE",
            "WAREHOUSE": "PERMISSION_BOT_WAREHOUSE",
            "ROLE": "PERMISSION_BOT",
        },
    }

    vars_dict = role_dict[role]

    conn_string = snowflake_URL(
        user=args[vars_dict["USER"]],
        password=args[vars_dict["PASSWORD"]],
        account=args[vars_dict["ACCOUNT"]],
        database=args[vars_dict["DATABASE"]],
        warehouse=args[vars_dict["WAREHOUSE"]],
        role=vars_dict["ROLE"],  # Don't need to do a lookup on this one
        schema=schema,
    )

    return create_engine(conn_string, connect_args={"sslcompression": 0})


def execute_query(engine: Engine, query: str) -> List[Tuple[Any]]:
    """
    Execute DB queries safely.
    """

    try:
        logging.warning(f"Running query on Snowflake: \n{query}")
        connection = engine.connect()
        results = connection.execute(query).fetchall()
    finally:
        connection.close()
        engine.dispose()
    return results


def execute_dataframe(engine, query):
    """Takes a query as a string argument and executes it.
    Results stored in dataframe if as_df = True."""
    cur = engine.raw_connection().cursor()

    try:
        results = cur.execute(query)
        df = pd.DataFrame.from_records(iter(results), columns=[x[0] for x in cur.description])
        return df
    except Exception as e:
        return print(f"Oh no! There was an error executing your query: {e}")
    finally:
        cur.close()
