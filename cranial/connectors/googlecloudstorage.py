import logging
import io
import os

from google.cloud.storage import Client, Blob
from google.oauth2 import service_account

from cranial.connectors import base
from cranial.common import logger

log = logger.get('GOOGLECLOUDSTORAGE_LOGLEVEL', name='gcs_connector')


class InMemoryConnector(base.Connector):
    def __init__(self, bucket, prefix='', binary=True, do_read=False,
                 credentials=None, project=None):
        super().__init__(base_address=prefix, binary=binary, do_read=do_read)

        params = {'project': project}
        if credentials:
            creds = service_account.Credentials.from_service_account_info(
                credentials)
            params['credentials'] = creds
        self.bucket = Client(**params).get_bucket(bucket)
        self.bucket_name = bucket

    def get(self, name=None):
        """
        Get a bytes stream of an object

        Parameters
        ----------
        name
            if None, prefix will be used as a key name, otherwise name will be added to prefix after '/' separator

        Returns
        -------
            bytes stream with key's data, or an empty stream if errors occurred
        """
        key = self.base_address if name is None else os.path.join(self.base_address, name)
        key = key.strip('/')

        try:
            blob = Blob(key, self.bucket)
            filestream = io.BytesIO()
            blob.download_to_file(filestream)
            filestream.seek(0)
            log.info("Getting {} bytes from \tbucket={}\tkey={}".format(
                blob.size, self.bucket_name, key))
            self.metadata = blob.metadata
        except Exception as e:
            log.error("{}\tbucket={}\tkey={}".format(e, self.bucket_name, key))
            if log.level == logging.DEBUG:
                raise e

        if self.do_read:
            asbytes = filestream.read()

            if not self.binary:
                return asbytes.decode()
            else:
                return asbytes

        return filestream

    def put(self, source, name=None):
        """

        Parameters
        ----------
        source
        name

        Returns
        -------

        """

        if isinstance(source, io.BytesIO):
            filebuff = io.BufferedReader(source)
        elif isinstance(source, (str, bytes)):
            filebuff = io.BufferedReader(io.BytesIO(source))
        else:
            log.error('Source should be either a string, or bytes or io.BytesIO, got {}'.format(type(source)))
            return False

        key = self.base_address if name is None else os.path.join(
            self.base_address, name)
        key = key.strip('/')
        try:
            blob = Blob(key, self.bucket)
            blob.upload_from_file(filebuff, rewind=True)
            log.info("Uploaded {} bytes to \tbucket={}\tkey={}".format(
                len(source), self.bucket_name, key))
            return True
        except Exception as e:
            log.error("{}\tbucket={}\tkey={}".format(e, self.bucket, key))
            return False
