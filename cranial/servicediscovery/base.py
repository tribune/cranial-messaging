from abc import ABCMeta, abstractmethod


class Discovery(metaclass=ABCMeta):
    def __init__(self, namespace):
        self.namespace = namespace
        self.prefix = self.namespace  # Backward compatibility.
        self.services = {}
        self.update()

    @abstractmethod
    def update(self):
        raise Exception('Not Implemented')

    def get_metadata(self, service: str, key: str):
        assert service in self.services
        return self.services[service].get(key)

    def get_instances(self, service: str):
        return self.get_metadata(service, 'hosts')

    def get_protocol(self, service: str):
        return self.get_metadata(service, 'protocol')
