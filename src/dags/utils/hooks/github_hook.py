"""
API reference: https://clockify.me/developers-api
"""

from airflow.hooks.base import BaseHook
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import logging
from typing import Optional, Union

from requests.models import Response


class GitHubHook(BaseHook):

    def __init__(self,
                 conn_id: Optional[Union[str, None]] = None,
                 *args, **kwargs):
        if conn_id is None:
            conn_id = 'github-comments'  # default conn_id
        conn = self.get_connection(conn_id)

        self.host = conn.host
        self._api_key = conn.password
        self._auth_headers = {'Authorization': f'Bearer {self._api_key}'}

        logging.info(f"GitHub hook initialized with API key '{self._api_key[:5]}***' ")

        super().__init__(*args, **kwargs)

    def create_issue_comment(self, repo: str, issue_number: int, body: str, owner: str = 'OsomePteLtd') -> Response:
        """
        Docs
        https://docs.github.com/en/rest/issues/comments?apiVersion=2022-11-28#create-an-issue-comment

        :param owner:
        :param repo:
        :param issue_number:
        :param body:
        :return:
        """

        endpoint = f'{self.host}/repos/{owner}/{repo}/issues/{issue_number}/comments'
        headers = self._auth_headers
        headers['Accept'] = 'application/vnd.github+json"'

        response = requests.post(
            url=endpoint,
            headers=headers,
            json={'body': body}
        )

        return response
