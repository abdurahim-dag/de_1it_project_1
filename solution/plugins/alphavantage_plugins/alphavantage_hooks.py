"""Хуки обращений к API."""
from airflow.hooks.http_hook import HttpHook


class StocksIntraDayHook(HttpHook):
    """Interact with TIME_SERIES_INTRADAY Stocks API."""

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

        url = f"query?function={function}&interval={self.interval}&outputsize={outputsize}&symbol={self.symbol}&apikey={self.apikey}"

        # Возвращаем json из данных по API
        return self.run(url).json()


class StocksIntraDayExtendedHook(HttpHook):
    """Interact with Intraday (Extended History) Stocks API."""

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

        _slice = 'year1month3'
        adjusted = 'false'

        url = f"query?function={function}&interval={self.interval}&adjusted={adjusted}&slice={_slice}&symbol={self.symbol}&apikey={self.apikey}"

        # Возвращаем response ответ от API.
        return self.run(url)
