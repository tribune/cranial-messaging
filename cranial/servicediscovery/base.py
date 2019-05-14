from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Union  # noqa

import yaml

ServiceName = str
ServiceValue = Union[str, List[str]]
ServiceDefinition = Dict[str, ServiceValue]
ServiceRegistry = Dict[ServiceName, ServiceDefinition]


class Discovery(metaclass=ABCMeta):
    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self.prefix = self.namespace  # Backward compatibility.
        self.services = {}  # type: ServiceRegistry
        self.update()

    @abstractmethod
    def update(self):
        raise Exception('Not Implemented')

    def get_metadata(self, service: str, key: str) -> ServiceValue:
        assert service in self.services
        return self.services[service][key]

    def get_instances(self, service: str) -> List[str]:
        return self.get_metadata(service, 'hosts')  # type: ignore

    def get_protocol(self, service: str) -> str:
        return self.get_metadata(service, 'protocol')  # type: ignore

    def get_mode(self, service: str) -> str:
        mode = self.get_metadata(service, 'mode')
        assert mode in ['any', 'all']
        return mode  # type: ignore


class YamlDiscovery(Discovery):
    def update(self):
        with open(self.namespace) as f:
            self.services = yaml.full_load(f)
