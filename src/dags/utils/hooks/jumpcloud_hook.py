"""
API reference: https://docs.jumpcloud.com/api/1.0/index.html#section/Overview
"""

from airflow.hooks.base import BaseHook
import requests
import logging
from typing import Optional, Union


class JumpcloudHook(BaseHook):

    def __init__(self,
                 *args, **kwargs):
        conn = self.get_connection('jumpcloud')

        self.host = conn.host
        self._api_key = conn.password
        self._auth_headers = {'x-api-key': self._api_key}

        logging.info(f"Jumpcloud hook initialized with API key '{self._api_key[:5]}***'.")
        super().__init__(*args, **kwargs)

    def systems_request(self,
                        skip: int = 0,
                        limit: int = 10,
                        fields: Optional[Union[str, None]] = None,
                        search: Optional[Union[str, None]] = None,
                        sort: Optional[Union[str, None]] = None,
                        filter_val: Optional[Union[str, None]] = None
                        ) -> requests.models.Response:
        """
        This endpoint returns all Systems

        Method docs:
        https://docs.jumpcloud.com/api/1.0/index.html#tag/Systems



        :param limit: The number of records to return at once. Limited to 100.
        :param skip: The offset into the records to return, should be >= 0
        :param fields: Use a space seperated string of field parameters to include the data in the response. If omitted,
         the default list of fields will be returned
        :param search: A nested object containing a searchTerm string or array of strings and a list of fields to search
         on.
        :param sort: Use space separated sort parameters to sort the collection. Default sort is ascending. Prefix with
        - to sort descending.
        :param filter_val: A filter to apply to the query. See the supported operators below. For more complex searches,
         see the related /search/<domain> endpoints, e.g. /search/systems.
         Read more in API docs.
        :return: Response contains dict with keys ['totalCount', 'results']

        """

        systems_endpoint = '/systems'

        params = {
            "limit": str(limit),
            "skip": str(skip),
        }

        if fields:
            params['fields'] = fields

        if search:
            params['search'] = search

        if sort:
            params['sort'] = sort

        if filter_val:
            params['filter'] = filter_val

        logging.info(f'Requesting a systems with following parameters {params}')
        response = requests.get(url=self.host + systems_endpoint, headers=self._auth_headers, params=params)

        logging.info(f'Status code: {response.status_code}, returning a response object')

        return response

    def get_full_systems_list(self) -> list:

        full_result_list = []
        total_count = -1
        skip_val = 0

        while skip_val != total_count:
            response = self.systems_request(
                skip=skip_val,
                limit=100)

            if not response.ok:
                logging.error(f'Something is wrong with the request - {response.reason}')
                raise requests.exceptions.RequestException(f'Status code {response.status_code}')

            response_json = response.json()

            total_count = response_json['totalCount']

            results_list = response_json['results']
            results_list_len = len(results_list)

            full_result_list.extend(results_list)
            logging.info(f'Extracted {results_list_len} records, total length is {len(full_result_list)}')

            skip_val += results_list_len

        logging.info('Extracting is done')

        return full_result_list
