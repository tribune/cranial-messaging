from time import sleep

from cranial.common.config import parse_config, factory # @TODO

config = parse_config() # @TODO

listener = factory(config['listener'])
notifier = factory(config['notifier'])

while True:
    message = listener.recv()
    if message:
        for target in config['targets']:
            notifier.send(message=message, **target)
    else:
        sleep(1)
