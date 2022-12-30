"""
API reference: https://clockify.me/developers-api
"""

from airflow.hooks.base import BaseHook

from datetime import datetime
from typing import Optional, Union
from requests.models import Response

import jwt
import time
import json
import logging
import requests

from utils.utils import get_workdir_in_data_folder


class GitHubHook(BaseHook):

    def __init__(self,
                 conn_id: Optional[Union[str, None]] = None,
                 *args, **kwargs):
        if conn_id is None:
            conn_id = 'github-comments'  # default conn_id
        conn = self.get_connection(conn_id)

        self.host = conn.host

        # authentication related
        self._make_private_key()
        self._app_id = conn.extra_dejson['app_id']
        self._app_installation_id = conn.extra_dejson['installation_id']
        self._access_token_filepath = get_workdir_in_data_folder('hooks/github/') + 'access_token.json'

        logging.info(f"GitHub hook initialized with private key '{self._private_key[:45]}***{self._private_key[-45:]}' "
                     f"and installation_id {self._app_installation_id} ")

        super().__init__(*args, **kwargs)

    @property
    def _auth_headers(self):
        logging.info('Reading access token from GCS')
        try:
            with open(self._access_token_filepath) as f:
                token = json.load(f)

                if datetime.fromisoformat(token['expires_at'][:-1]) < datetime.utcnow():
                    logging.warning(f'Token got expired, raising FileNotFoundError')
                    raise FileNotFoundError

        except FileNotFoundError:
            token = self._refresh_access_token()

        token = token['token']

        return {'Authorization': f'Bearer {token}'}

    def _refresh_access_token(self) -> dict:
        logging.info(f'Refreshing GH token')
        current_time = int(time.time())
        payload = {
            'iat': current_time,
            'exp': current_time + (1 * 60),
            'iss': self._app_id,
        }

        private_key = self._private_key.encode()

        encoded = jwt.encode(payload, private_key, algorithm='RS256')
        jwt_token = encoded.decode()

        url = f"{self.host}/app/installations/{self._app_installation_id}/access_tokens"

        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }

        response = requests.request("POST", url, headers=headers)

        access_token = response.json()

        with open(self._access_token_filepath, 'w+') as f:
            json.dump(access_token, f)

        logging.info(f'New token is valid until {str(access_token["expires_at"])}')

        return access_token

    def _make_private_key(self):
        with open(get_workdir_in_data_folder('hooks/github/') + 'somefile.txt', 'r') as f:
            private_key = f.read()

        self._private_key = private_key

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
