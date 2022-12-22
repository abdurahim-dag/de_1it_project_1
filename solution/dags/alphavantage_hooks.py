from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook
import logging

class StocksIntraDayHook(HttpHook):
    """
    Interact with Stocks API.
    """

    def __init__(
        self,
        http_conn_id: str,
        symbol: str,
        interval: str,
        apikey: str,
        **kwargs
    ) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'
        self.symbol = symbol
        self.apikey = apikey
        self.interval = interval

    def get_time_series(self):

        function = 'TIME_SERIES_INTRADAY'
        outputsize = 'full'

        url=f"query?function={function}&interval={self.interval}&outputsize={outputsize}&symbol={self.symbol}&apikey{self.apikey}"

        """Returns count of page in API"""
        return self.run(url).json()['results']


class StocksIntraDayExtendedHook(HttpHook):
    """
    Interact with Stocks API.
    """

    def __init__(
            self,
            http_conn_id: str,
            symbol: str,
            interval: str,
            apikey: str,

            **kwargs
    ) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'
        self.symbol = symbol
        self.apikey = apikey
        self.interval = interval

    def get_time_series(self):
        function = 'TIME_SERIES_INTRADAY_EXTENDED'

        slice = 'year1month3'
        adjusted = 'false'

        url = f"query?function={function}&interval={self.interval}&adjusted={adjusted}&slice={slice}&symbol={self.symbol}&apikey={self.apikey}"

        return self.run(url)
