"""
Expose your connectors here so they are all importable from cranial.connectors.
"""
from cranial.connectors.base import Connector
from cranial.connectors.local import Connector as LocalConnector
from cranial.connectors.s3 import InMemoryConnector as S3InMemoryConnector
