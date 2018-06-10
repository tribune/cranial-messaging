"""
expose your connectors here so they are all importable from cranial.fetchers
"""
from cranial.fetchers.connector import Connector
from cranial.fetchers.local import Connector2 as LocalConnector
from cranial.fetchers.s3 import InMemoryConnector as S3InMemoryConnector