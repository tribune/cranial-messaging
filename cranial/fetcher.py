"""
A Fetcher is a the combination of a Connector and a Parser, providing a
standard way to retrieve data records as Dictionaries from arbitrary storage
systems, via a generator.
"""
from cranial.connectors.base import Connector
from cranial.parsers import base, line


class Parser():
    pass


class Fetcher():
    def __init__(self, connector: Connector, parser: base.Parser=line.Parser) -> None:
        self.connector = connector
        self.parser = parser

    def __iter__(self):
        return self.generator()

    def generator(self):
        for record in self.parser(self.connector):
            yield record
