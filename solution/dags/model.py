from dataclasses import dataclass


@dataclass()
class Stock:
    time: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    upload_id: str
