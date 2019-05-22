import requests
from time import time
from typing import Any, Callable, Dict, List, Optional  # noqa

from cranial.common import logger
from cranial.servicediscovery import base

log = logger.get()

MARATHON_URL = 'http://marathon.mesos:8080/v2/apps'


def get_services_with_predicate(predicate: Callable) -> List:
    """Return a list of all Marathon Services that satisfy the predicate."""
    response = requests.get(MARATHON_URL)
    if response.status_code == requests.codes.ok:
        services = [x for x in response.json()['apps'] if predicate(x)]
        if log:
            log.info('Got {} services from Marathon.'.format(len(services)))
        return services
    else:
        if log:
            log.warn('Bad response from Marathon Service Discovery.')
        return []


def get_tasks_for_service(service_id: str, portIndex: int = 0) -> List[str]:
    """Return a list of ip:portIndex for all tasks belonging to the service.

    The service_id is the string including the leading /, as given by the 'id'
    field for the service definition.
    """
    response = requests.get(MARATHON_URL + service_id)
    app = response.json()['app']
    result = []
    for task in app['tasks']:
        # Assume healthy if health checks aren't defined.
        healthy = True
        if len(app.get('healthChecks', [])):
            # Not sure about just checking [0] here, but its better than the
            # nothing we had before.
            healthy = task.get('healthCheckResults',
                               [{'alive': False}])[0]['alive']

        if task['state'] == 'TASK_RUNNING' and healthy:
            result.append('{}:{}'.format(task['host'],
                                         task['ports'][portIndex]))
    return result


def get_tasks_by_label(label: str, log='IGNORED') -> dict:
    """Return a nested dict of lists of ip:portIndex for all tasks of all
    services having a value for the given DCOS label, keyed by that value.

    Parameters
    ----------

    label - Marathon Service Label to look for.

    log - Legacy Parameter, ignored.

    Return value
    ------------
    A nested dictionary of the form:
        {'$value': {'$service_id': ['$ip:$port', ...], ...}, ...}
    """
    result = {}  # type: Dict[str, Dict[str, List[str]]]
    services = get_services_with_predicate(
        lambda x: label in x['labels'].keys())
    if services:
        service_ids = [x['id'] for x in services]
        for s in service_ids:
            response = requests.get(MARATHON_URL + s)
            app = response.json()['app']
            value = app['labels'][label]
            if value not in result:
                result[value] = {}
            result[value][s] = []
            for task in app['tasks']:
                address = task['host']
                # @TODO fix this hack for rabbitmq.
                if s == '/rabbitmq':
                    address += ':5672'
                elif len(task.get('ports', [])):
                    address += ':' + str(task['ports'][0])
                result[value][s].append(address)
    return result


def get_service_port(hostname: str, port_only=False):
    # Do the import here for now because dnspython is not widely deployed.
    import dns.resolver
    parts = hostname.split('.')
    srvname = '.'.join(['_' + parts[0]] + ['_tcp'] + parts[1:])
    port = dns.resolver.query(srvname, 'SRV')[0].port
    return port if port_only else '{}:{}'.format(hostname, port)


class Discovery(base.Discovery):
    time = 0

    def update(self):
        # Don't spam the service discovery.
        if time() - self.time < .5:
            return

        self.time = time()
        services = get_services_with_predicate(
          lambda x: self.namespace in x['labels'].keys())
        for s in services:
            response = requests.get(MARATHON_URL + s.id)
            app = response.json()['app']
            mode = app['labels'][self.namespace]
            proto = app['labels'].get('NOTIFIER', 'kafka')
            self.services[s.id] = {'mode': mode,
                                   'protocol': proto,
                                   'hosts': []}
            for task in app['tasks']:
                address = task['host']
                # @TODO fix this hack for rabbitmq.
                if s == '/rabbitmq':
                    address += ':5672'
                elif len(task.get('ports', [])):
                    address += ':' + str(task['ports'][0])
                self.services[s.id]['hosts'].append(address)

        log.debug(self.services)
