def file_readlines(fh, delete_after=False):
    """
    memory efficient iterator to read lines from a file (readlines() method reads whole file)
    Parameters
    ----------
    fp
        path to a decompressed file downloaded from s3 key
    delete_after
        delete file after it was read
    Returns
    -------
        generator of lines
    """
    while True:
        line = fh.readline()
        if line:
            yield line
        else:
            break

    if delete_after:
        try:
            os.unlink(fp)
        except Exception as e:

            log.warning(e)


class Fetcher():
    """A Record Parser is a Generator function that takes a filelike object
    and iterates single records."""

    def __init__(self, data_source_id: str, RecordParser=file_readlines):
        self.connector = RecordParser(self._get_connector(data_source_id))

    def get_iteratable(self, data_source_id) -> iter:
        # @todo Not yet complete.
        return self.connector.get(data_source_id)

    @staticmethod
    def _get_connector(data_source_id: str):
        proto = data_source_id.split(':')[0]
        if proto == 's3':
            from . import s3
            return s3.S3Connector()

        if proto.startswith('http'):
            from . import http
            return http.Connector()
