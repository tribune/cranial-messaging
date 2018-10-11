import io
import logging
import os
import shutil
import subprocess
import tempfile
from typing import Dict

import boto3

from cranial.connectors import base
from cranial.common import logger

log = logger.get('S3_LOGLEVEL', name='s3_fetchers')

SOURCE_DIR = 'storage/source'
TARGET_DIR = 'storage/target'
MODEL_DIR = 'storage/model'


def cleanup_temp_data():
    """
    delete temp dirs
    """
    try:
        shutil.rmtree(SOURCE_DIR)
        log.info("Removed {}".format(SOURCE_DIR))
    except Exception as e:
        log.info(e)
    try:
        shutil.rmtree(TARGET_DIR)
        log.info("Removed {}".format(TARGET_DIR))
    except Exception as e:
        log.info(e)
    try:
        shutil.rmtree(MODEL_DIR)
        log.info("Removed {}".format(MODEL_DIR))
    except Exception as e:
        log.info(e)


def prepare_s3_prefix(bucket, pre_date_part, y=None, mo=None, d=None, h=None, after_date_part=''):
    """
    compose an s3 path from given parts and date values
    example:
        {bucket}/{pre_date_part}/mo={y}-{mo}/day={d}/hour={h}/after_date_part
    Parameters
    ----------
    bucket
        bucket name, may include "s3://"
    pre_date_part
        part of the path before date strings
    y
        year number
    mo
        month number
    d
        day number
    h
        hour number
    after_date_part
        part of the path after date strings
    Returns
    -------
        composed s3 path
    """
    if not bucket.startswith('s3://'): bucket = 's3://' + bucket
    s = '/'.join([bucket.strip('/'), pre_date_part.strip('/')])
    if y is not None and mo is not None:
        s += '/mo={y}-{mo}'.format(y=y, mo=mo)
    if d is not None:
        s += '/day={d}'.format(d=d)
    if h is not None:
        s += '/hour={h}'.format(h=h)
    if len(after_date_part) > 0:
        s += '/' + after_date_part

    return s


def read_key(key, bucket, decode=True, verbose=False):
    """
    The intended use is within a map function, so to change bucket and verbosity is not extremely simple and needs
    some functools (partial?)

    Parameters - self explanatory

    Returns - contents of the s3 key, decoded into utf8 string
    """
    if verbose:
        log.info('reading {}'.format(key))
    try:
        b_str = boto3.resource('s3').Bucket(bucket).Object(key).get()['Body'].read()
    except Exception as e:
        log.error("{}:{}\t{}".format(bucket, key, e))
        return None
    return b_str.decode() if decode else b_str


def key_download_decompress(bucket, key, force_decompress=False):
    """
    First step in getting events from s3 key:
        - download a key into a temp file
        - decompress on disk
    Returns
    -------
        name of the decompressed temp file or None if failed
    """
    fp = tempfile.mkstemp(suffix='.gz')[1]
    boto3.resource('s3').Bucket(bucket).download_file(key, fp)
    log.info("downloaded {}/{} to {}".format(bucket, key, fp))
    if key.endswith('.gz') or force_decompress:
        # @TODO Replace with gzip module.
        p = subprocess.run(['gunzip', fp, '-f'], stderr=subprocess.PIPE)

        if p.returncode == 0:
            log.info('decompressed {}'.format(fp))
            return fp[:-3]
        else:
            log.warning(p.stderr)
            return None


class S3Connector(base.Connector):
    def __init__(self, bucket: str, prefix='') -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.cache = {}  # type: Dict[str, str]
        log.info('s3.S3Connector downloads whole files to local disk '
                 + 'before use. Consider s3.InMemoryConnector instead.')

    def get(self, s3_path, binary=False, redownload=False):
        if not redownload:
            try:
                local_path = self.cache[s3_path]
                return open(local_path, 'r' + 'b' if binary else '')
            except Exception as e:
                log.warn('S3 object {} not in cache: {}'.format(s3_path, e))

        local_path = key_download_decompress(self.bucket, self.prefix + s3_path)
        self.cache[s3_path] = local_path
        return open(local_path, 'r' + 'b' if binary else '')

    def put(self, source, name=None):
        fname = name if name else source.split('/')[-1]
        bucket = boto3.resource('s3').Bucket(self.bucket)
        if type(source) is str:
            bucket.upload_file(
                    self.prefix + fname, source)
            return True

        elif type(source) is io.BytesIO:
            bucket.upload_fileobj(
                    self.prefix + fname, source)
            return True

        else:
            raise Exception(
                "Can't put source data type {}. Pass BytesIO or string path to a local file.".format(type(source)))


class InMemoryConnector(base.Connector):
    def __init__(self, bucket, prefix='', binary=True, do_read=False,
                 credentials={}):
        super(InMemoryConnector, self).__init__(base_address=prefix, binary=binary, do_read=do_read)
        self.credentials = credentials
        self.bucket = bucket

    def get(self, name=None):
        """
        Get a bytes stream of an s3 key

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
            res = boto3.resource('s3', **self.credentials).Bucket(self.bucket).Object(key).get()
            loglevel = log.warning if res['ContentLength'] == 0 else log.debug
            loglevel("Getting {} bytes from \tbucket={}\tkey={}".format(res['ContentLength'], self.bucket, key))
            body = res.pop('Body')
            self.metadata = res
        except Exception as e:
            log.error("{}\tbucket={}\tkey={}".format(e, self.bucket, key))
            if log.level == logging.DEBUG:
                raise e
            body = io.BytesIO()

        if self.do_read:
            body = body.read()

        if not self.binary:
            body = body.decode()

        return body

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
            source = source.read()
        elif isinstance(source, (str, bytes)):
            pass
        else:
            log.error('Source should be either a string, or bytes or io.BytesIO, got {}'.format(type(source)))
            return False

        key = self.base_address if name is None else os.path.join(self.base_address, name)
        key = key.strip('/')
        try:
            res = boto3.resource('s3').Bucket(self.bucket).Object(key).put(Body=source)
            log.info("Uploaded {} bytes to \tbucket={}\tkey={}".format(len(source), self.bucket, key))
            return res['ResponseMetadata']['HTTPStatusCode'] == 200
        except Exception as e:
            log.error("{}\tbucket={}\tkey={}".format(e, self.bucket, key))
            return False
