import requests
import json as jsonlib
import click
import csv as csvlib
import sys
from jinja2 import Template

def graphql_query(query):
    github_token = os.getenv('GITHUB_TOKEN')
    headers = {"Authorization": "Bearer {}".format(github_token)}
    request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    raise Exception("Query failed to run by returning code of {}.".format(request.status_code))

def gen_query(org, repo, cursor = ""):
    return Template("""
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
    """).render(repo=repo, cursor=cursor, org=org)
    
def gen_repo_query(org, cursor = ""):
    return Template("""
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
    """).render(cursor=cursor, org=org)

@click.group()
def cli():
    pass

def contributors():
    print("here")
    org = 'mattermost'
    data = []

    writer = csvlib.writer(sys.stdout)
    writer.writerow(["PR", "Merged At", "Author", "Repo"])

    cursor = ""
    result = graphql_query(gen_repo_query(org,cursor))
    repo = result["data"]["organization"]["repositories"]["nodes"]

    for one_repo in repo:
        has_next = True
        cursor = ""
        while has_next:
            query = gen_query(org, one_repo["name"] , cursor)

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
                    writer.writerow([node["number"], node["mergedAt"], node["author"]["login"],one_repo["name"]])

if __name__ == "__main__":
    contributors()
