import json as jsonlib
import pandas as pd
import requests
import snowflake.connector
import sys
from jinja2 import Template

from extract.utils import snowflake_engine_factory, execute_query, execute_dataframe


def graphql_query(query):
    github_token = os.getenv("GITHUB_TOKEN")
    headers = {"Authorization": "Bearer {}".format(github_token)}
    request = requests.post(
        "https://api.github.com/graphql", json={"query": query}, headers=headers
    )
    if request.status_code == 200:
        return request.json()
    raise Exception(
        "Query failed to run by returning code of {}.".format(request.status_code)
    )


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
    data = []

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
        has_next = result["data"]["organization"]["repositories"]["pageInfo"][
            "hasNextPage"
        ]
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
                        [
                            [
                                node["number"],
                                node["mergedAt"],
                                node["author"]["login"],
                                one_repo,
                            ]
                        ]
                    )

    df = pd.DataFrame(records, columns=["PR_Number", "Merged_At", "Author", "Repo",])

    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
    connection = engine.connect()

    df.to_sql(
        "github_contributions_all",
        con=connection,
        index=False,
        schema="staging",
        if_exists="replace",
    )


if __name__ == "__main__":
    contributors()
