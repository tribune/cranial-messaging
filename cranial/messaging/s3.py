import json
import os
from typing import Dict, IO  # noqa

from cranial.messaging.file import Notifier as FileNotifier
from cranial.common import logger

from datetime import datetime, timedelta

log = logger.get()


class Notifier(FileNotifier):
    """ Write messages to an s3 object named `endpoint`.
    """

    def parts_to_path(address: str, endpoint: str) -> str:
        """ Provides URI string based configurability, per cranial.common.config

        file:///foo/bar is absolute;
        file://./foo/bar is relative.
        """
        return 's3://' + address + '/' + endpoint

    def get_last_id(self,  bucket: str = None,
                    prefix: str = None, serde=json,**kwargs):
        """ Takes an S3 endpoint and the seperator used in naming files
            gets all the files at the prefix in the given endpoint
            gets the most recently modified file with an id
            reads the last row from that file and returns that id as the
            last_id
        """

        date_format = kwargs.get('date_format', '%Y/%m/%d')

        if kwargs.get('address', '') is not '':
            endpoint = 's3://' + kwargs.get('address') + '/' + kwargs.get('endpoint')
        else:
            endpoint = kwargs.get('path', '')

        try:
            try:
                date_path = datetime.now().strftime(date_format)
                keys = FileConnector(endpoint + '/' + date_path).get_dir_keys(bucket=bucket, prefix=prefix)
            except Exception:
                try:
                    prev_date_path = (datetime.now() - timedelta(days=1)).strftime(date_format)
                    keys = FileConnector(endpoint).get_dir_keys(bucket=bucket, prefix=prefix + '/' + prev_date_path)
                except Exception:
                    keys = FileConnector(endpoint).get_dir_keys(bucket=bucket, prefix=prefix)

            sorted_keys = sorted(keys, key=lambda item: item['LastModified'], reverse=True)

            adr = 's3://' + endpoint.split('//')[1].split('/')[0] + '/'
            last_file = FileConnector(adr + sorted_keys[0]['Key']).get()
            # todo default to json parsing if no parser is provided
            last_row = json.loads(last_file.read().split('\n')[-2])
            # todo dynamically tell what key id is under or pass it in
            last_id = list(last_row.values())[0]

        except Exception as e:
            raise base.NotifyException(
                "{} || endpoint: {}".format(
                    e, endpoint))

        return int(last_id)
