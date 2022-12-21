import os

import pandas as pd
import requests
from jinja2 import Template
from snowflake.connector.pandas_tools import pd_writer

from extract.utils import snowflake_engine_factory


def graphql_query(query):
    github_token = os.getenv("GITHUB_TOKEN")
    headers = {"Authorization": "Bearer {}".format(github_token)}
    request = requests.post("https://api.github.com/graphql", json={"query": query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    raise Exception("Query failed to run by returning code of {}.".format(request.status_code))


def gen_query(org, repo, cursor=""):
    return Template(
        """
    {
      repository(name: "{{repo}}", owner: "{{org}}") {
        name
        pullRequests(first: 100, states: [MERGED] {%if cursor %}, after: "{{cursor}}"{% endif %}) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            number
            mergedAt
            author {
              login
            }
          }
        }
      }
    }
    """
    ).render(repo=repo, cursor=cursor, org=org)


def gen_repo_query(org, cursor=""):
    return Template(
        """
    {
      organization(login: "{{org}}") {
        repositories(first: 100 {%if cursor %}, after: "{{cursor}}"{% endif %}) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            name
          }
        }
      }
    }
    """
    ).render(cursor=cursor, org=org)


def contributors():
    org = "mattermost"
    repo = []
    records = []
    has_next = True
    cursor = ""
    while has_next:
        try:
            result = graphql_query(gen_repo_query(org, cursor))
        except Exception as e:
            print(e)
            return

        repo_results = result["data"]["organization"]["repositories"]["nodes"]
        has_next = result["data"]["organization"]["repositories"]["pageInfo"]["hasNextPage"]
        cursor = result["data"]["organization"]["repositories"]["pageInfo"]["endCursor"]

        for i in repo_results:
            repo.append(i["name"])

    for one_repo in repo:
        has_next = True
        cursor = ""
        while has_next:
            query = gen_query(org, one_repo, cursor)

            try:
                result = graphql_query(query)
            except Exception as e:
                print(e)
                return

            pull_requests = result["data"]["repository"]["pullRequests"]
            has_next = pull_requests["pageInfo"]["hasNextPage"]
            cursor = pull_requests["pageInfo"]["endCursor"]

            for node in pull_requests["nodes"]:
                if node and node["author"]:
                    records.append(
                        {
                            "PR_NUMBER": node["number"],
                            "MERGED_AT": node["mergedAt"],
                            "AUTHOR": node["author"]["login"],
                            "REPO": one_repo,
                        }
                    )

    df = pd.DataFrame.from_records(records)

    try:
        engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
        connection = engine.connect()
        connection.execute("DELETE FROM staging.github_contributions_all")

        print(f"Preparing to load results to Snowflake. Records: {len(records)}")

        df.to_sql(
            "github_contributions_all",
            con=connection,
            index=False,
            schema="STAGING",
            if_exists="append",
            method=pd_writer,
        )
    except Exception as e:
        print(e)
        return
    finally:
        connection.close()


if __name__ == "__main__":
    contributors()
