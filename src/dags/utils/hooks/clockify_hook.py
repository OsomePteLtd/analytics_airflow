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


class ClockifyHook(BaseHook):

    def __init__(self,
                 workpace_id: Optional[Union[str, None]] = None,
                 *args, **kwargs):
        conn = self.get_connection('clockify')

        self.host = conn.host
        self._api_key = conn.password
        self.conn_extra = conn.extra_dejson
        self.reports_url = self.conn_extra['reports_url']
        self._auth_headers = {'X-Api-Key': self._api_key}

        if workpace_id:
            self.workspace_id = workpace_id
        else:
            self.workspace_id = self.conn_extra['workspace_id']

        logging.info(f"Clockify hook initialized with API key '{self._api_key[:5]}***' "
                     f"and workspace id {self.workspace_id}")

        super().__init__(*args, **kwargs)

    def _detailed_report_request(self,
                                 date_range_start: str = '2022-10-01T00:00:00.000',
                                 date_range_end: str = '2022-10-15T23:59:59.000',
                                 page_num: int = 1,
                                 page_size: int = 1000,
                                 export_type: str = 'CSV',
                                 custom_request_body: dict = None
                                 ) -> requests.models.Response:
        """
        Not complete wrapper for detailed report request API.
        reference: https://clockify.me/developers-api#operation--v1-workspaces--workspaceId--reports-detailed-post

        :param date_range_start: start date in ISO format YYYY-MM-DDTHH:MM:SS.000
        :param date_range_end: end date in ISO format YYYY-MM-DDTHH:MM:SS.000
        :param page_num: page number
        :param page_size: page size. Max value: 1000
        :param export_type: Possible values: 'JSON', 'CSV', 'XLSX', 'PDF'

        :param custom_request_body: if passed would override all other arguments
        :return: returns a response
        """

        detailed_report_endpoint = f'/workspaces/{self.workspace_id}/reports/detailed'

        if custom_request_body:
            body = custom_request_body
        else:
            body = {
                "dateRangeStart": date_range_start,
                "dateRangeEnd": date_range_end,
                "detailedFilter": {
                    "page": page_num,
                    "pageSize": page_size,
                },
                "exportType": export_type
            }

        response = requests.post(
            url=self.reports_url + detailed_report_endpoint,
            headers=self._auth_headers,
            json=body

        )

        return response

    def get_detailed_report_df(self,
                               start_date: str,
                               end_date: str,
                               ) -> pd.DataFrame:
        '''
        :param start_date: lower date bound in str format YYYY-MM-DD HH:MM:SS
        :param end_date: upper date bound in str format YYYY-MM-DD HH:MM:SS
        :return: pandas dataframe for the specified period
        '''

        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

        response = self._detailed_report_request(
            date_range_start=start_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
            date_range_end=end_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
            export_type='CSV'
        )

        if response.status_code != 200:
            logging.error(f'Something is wrong with the request - {response.reason}')
            raise requests.exceptions.RequestException(f'Status code {response.status_code}')

        raw_detailed_report = response.text

        df = pd.read_csv(StringIO(raw_detailed_report))
        logging.info(f'Report dataframe contains {len(df)} rows')

        return df
