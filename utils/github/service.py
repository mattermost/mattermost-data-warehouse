import csv
import logging
from typing import Callable, Dict, Iterable, TextIO

from ghapi.all import GhApi
from ghapi.page import paged

logger = logging.getLogger(__name__)


class GithubService:
    def __init__(self, token: str):
        self.api = GhApi(token=token)

    def repositories(self, organization: str, per_page: int = 30) -> Iterable[Dict]:
        """
        Iterate over all repository names for given organization.

        :param organization: the organization to get list of repositories.
        :param per_page: number of results to retrieve internally.
        """
        for page in paged(self.api.repos.list_for_org, org=organization, per_page=per_page):
            yield from page

    def pulls(self, organization: str, repo: str, state='closed', per_page: int = 30) -> Iterable[Dict]:
        """
        Iterate over all pull requests for a repo in an organization and return .

        :param organization: the organization the repo belongs to.
        :param repo: the repo to get pull requests from.
        :param state: the state of the pull requests to fetch. Default: closed, can be one of: open, closed, all.
        :param per_page: number of results to retrieve internally.
        """
        for page in paged(self.api.pulls.list, owner=organization, repo=repo, state=state, per_page=per_page):
            yield from page

    def download_pulls(
        self,
        organization: str,
        target: TextIO,
        state='closed',
        pr_filter: Callable = lambda x: True,
        per_page: int = 30,
    ) -> None:
        """
        Downloads all PR data for given organization and stores them as CSV to target file.

        :param organization: the organization to store data to.
        :param target: the file to write the CSV data to.
        :param state: the state of the pull requests to fetch. Default: closed, can be one of: open, closed, all.
        :param pr_filter: filter to apply to each PR in order to decide whether to add it or not. Default: include all.
        :param per_page: number of items to read per page when calling github's API.
        """
        writer = csv.DictWriter(target, fieldnames=['PR_NUMBER', 'MERGED_AT', 'AUTHOR', 'REPO'], extrasaction='ignore')
        writer.writeheader()
        logger.info('Starting downloading PRs from repositories')
        for repo in self.repositories(organization, per_page=per_page):
            logger.info(f'Start downloading closed PRs for repo {repo["name"]}')
            for pr in self.pulls(organization, repo['name'], state='closed', per_page=per_page):
                if pr_filter(pr):
                    writer.writerow(
                        {
                            'ORGANIZATION': organization,
                            'REPO': repo['name'],
                            'PR_NUMBER': pr['number'],
                            'AUTHOR': pr['user']['login'],
                            'MERGED_AT': pr['merged_at'],
                        }
                    )
            logger.info(f'Finished download PRs for repo {repo["name"]}')
