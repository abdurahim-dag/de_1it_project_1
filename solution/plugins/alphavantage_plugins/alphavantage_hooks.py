from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook


class StocksIntraDayHook(HttpHook):
    """
    Interact with Stocks API.
    """

    def __init__(
        self,
        http_conn_id: str,
        symbol: str,
        apikey: str,
        **kwargs
    ) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'
        self.symbol = symbol
        self.apikey = apikey

    def get_time_series(self):

        function = 'TIME_SERIES_INTRADAY'
        interval = '5min'
        outputsize = 'full'

        url=f"query?function={function}&interval={interval}&outputsize={outputsize}&symbol={self.symbol}&apikey{self.apikey}"

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
            apikey: str,
            **kwargs
    ) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'
        self.symbol = symbol
        self.apikey = apikey

    def get_time_series(self):
        function = 'TIME_SERIES_INTRADAY_EXTENDED'
        interval = '5min'
        slice = 'year1month3'
        adjusted='false'

        url=f"query?function={function}&interval={interval}&adjusted={adjusted}&slice={slice}&symbol={self.symbol}&apikey{self.apikey}"

        res = self.run(url)
        print(type(res))
        print(res)
        return
