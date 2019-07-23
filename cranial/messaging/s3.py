from typing import Dict, IO  # noqa

from cranial.messaging.file import Notifier as FileNotifier
from cranial.common import logger


log = logger.get()


class Notifier(FileNotifier):
    """ Write messages to an s3 object named `endpoint`.
    """

    def parts_to_path(self, address: str, endpoint: str) -> str:
        """ Provides URI-based configurability, per cranial.common.config
        """
        return 's3://' + address + '/' + endpoint
