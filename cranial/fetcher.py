"""
A Fetcher is a the combination of a Connector and a SerDe, providing a
standard way to retrieve data records as Dictionaries from arbitrary storage
systems, via a generator.
"""
from typing import Dict, Iterator

import ujson

from cranial.connectors.base import Connector
from cranial.messaging.base import Serde


class Fetcher():
    def __init__(self, connector: Connector, serde: Serde = ujson) -> None:
        self.connector = connector
        self.serde = serde

    def __iter__(self):
        return self.generator()

    def generator(self, target) -> Iterator[Dict]:
        for record in self.connector.iterator(target):
            yield self.serde.loads(record)
