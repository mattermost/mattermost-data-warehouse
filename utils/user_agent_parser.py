from user_agents import parse
import os
import pandas as pd
import sys
from extract.utils import snowflake_engine_factory, execute_query, execute_dataframe
import math


def parse_user_agent():
    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
    """ This function searches for and parses all available user agents received via telemetry data that are
    not currently in the analytics.mattermost.user_agent_registry table."""

    # CREATE USER_AGENT_REGISTRY IF IT DOES NOT ALREADY EXIST.
    query = f"""
    CREATE TABLE IF NOT EXISTS analytics.WEB.user_agent_registry
(
    context_useragent VARCHAR,
    browser           VARCHAR,
    browser_version   VARCHAR,
    operating_system  VARCHAR,
    os_version        VARCHAR,
    device_type       VARCHAR,
    device_brand      VARCHAR,
    device_model      VARCHAR
);"""

    execute_query(engine, query)

    # UNION ALL SOURCES OF CONTEXT_USERAGENT DATA THAT ARE NOT CURRENTLY IN THE USER_AGENT_REGISTRY TABLE.
    query = f"""
    SELECT *
    FROM (
        SELECT CONTEXT_USER_AGENT AS CONTEXT_USERAGENT
        FROM RAW.MATTERMOST2.EVENT
        WHERE CONTEXT_USER_AGENT IS NOT NULL
            AND CONTEXT_USER_AGENT NOT IN (SELECT CONTEXT_USERAGENT FROM analytics.WEB.user_agent_registry GROUP BY 1)
            AND TIMESTAMP::DATE >= CURRENT_DATE - INTERVAL '1 DAY'
        GROUP BY 1
        UNION ALL
        SELECT CONTEXT_USERAGENT
        FROM RAW.MM_TELEMETRY_PROD.EVENT
        WHERE CONTEXT_USERAGENT IS NOT NULL
            AND CONTEXT_USERAGENT NOT IN (SELECT CONTEXT_USERAGENT FROM analytics.WEB.user_agent_registry GROUP BY 1)
            AND TIMESTAMP >= '2020-05-20'
        GROUP BY 1
        UNION ALL
        SELECT USERAGENT AS CONTEXT_USERAGENT
        FROM RAW.RELEASES.LOG_ENTRIES
        WHERE USERAGENT IS NOT NULL
            AND USERAGENT NOT IN (SELECT CONTEXT_USERAGENT FROM analytics.WEB.user_agent_registry GROUP BY 1)
            AND LOGDATE::date >= CURRENT_DATE - INTERVAL '1 DAY'
        GROUP BY 1
        UNION ALL
        SELECT USERAGENT AS CONTEXT_USERAGENT
        FROM RAW.DIAGNOSTICS.LOG_ENTRIES
        WHERE USERAGENT IS NOT NULL
            AND USERAGENT NOT IN (SELECT CONTEXT_USERAGENT FROM analytics.WEB.user_agent_registry GROUP BY 1)
            AND LOGDATE::date >= CURRENT_DATE - INTERVAL '1 DAY'
        GROUP BY 1
        UNION ALL
        SELECT CONTEXT_USERAGENT
        FROM raw.mattermostcom.pages
        WHERE CONTEXT_USERAGENT IS NOT NULL
            AND CONTEXT_USERAGENT NOT IN (SELECT CONTEXT_USERAGENT FROM analytics.WEB.user_agent_registry GROUP BY 1)
            AND TIMESTAMP::DATE >= CURRENT_DATE - INTERVAL '1 DAY'
        GROUP BY 1
        )
    WHERE CONTEXT_USERAGENT NOT IN (SELECT CONTEXT_USERAGENT FROM analytics.WEB.user_agent_registry GROUP BY 1)
    GROUP BY 1;
    """

    df = execute_dataframe(engine, query)

    if (
        len(df) == 0
    ):  # CHECKS TO SEE IF THERE ARE ANY NEW CONTEXT_USERAGENTS TO INSERT INTO THE TABLE
        print("Nothing to do.")
    else:  # PARSES USERAGENT COMPONENTS AND APPENDS EACH COMPONENT AS A COLUMN TO THE EXISTING DATAFRAME.
        browser = []
        browser_family = []
        browser_version = []
        browser_version_string = []
        operating_system = []
        os_family = []
        os_version = []
        os_version_string = []
        device = []
        device_family = []
        device_brand = []
        device_model = []

        for index, row in df.iterrows():
            ua_string = row["CONTEXT_USERAGENT"]
            user_agent = parse(ua_string)

            browser.append(user_agent.browser)
            browser_family.append(user_agent.browser.family)
            browser_version.append(user_agent.browser.version)
            browser_version_string.append(user_agent.browser.version_string)

            # Accessing user agent's operating system properties
            operating_system.append(user_agent.os)
            os_family.append(user_agent.os.family)
            os_version.append(user_agent.os.version)
            os_version_string.append(user_agent.os.version_string)

            # Accessing user agent's device properties
            device.append(user_agent.device)
            device_family.append(user_agent.device.family)
            device_brand.append(user_agent.device.brand)
            device_model.append(user_agent.device.model)

        browser = pd.Series(browser_family, name="browser")
        browser_version = pd.Series(browser_version_string, name="browser_version")
        op_sys = pd.Series(os_family, name="operating_system")
        os_version = pd.Series(os_version_string, name="os_version")
        device_type = pd.Series(device_family, name="device_type")
        device_brand = pd.Series(device_brand, name="device_brand")
        device_model = pd.Series(device_model, name="device_model")

        agent_lists = [
            browser,
            browser_version,
            op_sys,
            os_version,
            device_type,
            device_brand,
            device_model,
        ]
        for item in agent_lists:
            df = df.join(item)

        connection = engine.connect()

        # 16,384 is Snowflake Insert statement row limit. To ensure the job executes successfully we use the below code to check that the data being inserted
        # is not more than the allowed row limit. If it is, we incrementally load the dataframe.
        df[0 : 16384 if len(df) > 16384 else len(df)].to_sql(
            "user_agent_registry",
            con=connection,
            index=False,
            schema="WEB",
            if_exists="append",
        )
        i = 2  # The default number of times to increment. Will autoincrement if more than 2 inserts are required.

        if i <= math.ceil(len(df) / 16384):
            x = 16384  # The start row of the dataframe slice to be inserted. Will autoincrement if more than 2 inserts are required.
            y = (
                16384 * 2
            )  # The end row of the dataframe slice to be inserted. Will autoincrement if more than 2 inserts are required.

            # Loops through the remaining insert statements required to finish the job i.e. load all new user agents found in the mattermostcom.pages table.
            for n in range(math.ceil(len(df) / 16384) - 1):
                df[x : y if y < len(df) else len(df)].to_sql(
                    "user_agent_registry",
                    con=connection,
                    index=False,
                    schema="WEB",
                    if_exists="append",
                )
                x = y
                y += 16384
                i += 1
        return print(
            f"""Successfully uploaded {len(df)} records to mattermost.user_agent_registry!"""
        )


if __name__ == "__main__":
    parse_user_agent()
