import pickle

import boto3

from cranial.common import logger

firehose = None
log = logger.get()
stored_keys = None


def auth(keys: dict):
    global stored_keys
    stored_keys = keys


def get_client(aws_keys: dict = None, new=False):
    global firehose
    if aws_keys is None:
        global stored_keys
        aws_keys = stored_keys
    if firehose is not None and not new:
        return firehose
    firehose = boto3.client('firehose',
                            aws_access_key_id=aws_keys['key'],
                            aws_secret_access_key=aws_keys['secret'],
                            region_name=aws_keys['region_name']) if aws_keys \
                                    else boto3.client('firehose')
    return firehose


def put_data(stream: str, data):
    """Encodes data if it's not already bytes & delivers it."""
    firehose = get_client()

    if type(data) is str:
        # Add newline as record seperator if not present.
        if data[-1] != '\n':
            data += '\n'
        bits = bytes(data, 'utf-8')
    else:
        try:
            bits = bytes(data)
        except Exception as e:
            log.warning(e)
            bits = pickle.dumps(data)

    record = {'Data': bits}
    try:
        return firehose.put_record(DeliveryStreamName=stream, Record=record)
    # Retry once in case of dead client.
    except Exception as first_err:
        log.warn(str(first_err))
        firehose = get_client(new=True)
        try:
            return firehose.put_record(DeliveryStreamName=stream, Record=record)
        except Exception as e:
            log.error(str(e))
            raise e
