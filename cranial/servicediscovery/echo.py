"""
A dumb service discovery function mirroring the ugly legacy
marathon.get_tasks_for_service, created for integration testing.
"""

def single_service(protocol, host, port=None):
    address = host if not port else host + ':' + port

    def pd(*args, **kwargs):
        return {protocol: {'service': [address] }}

    def sd(*args, **kwargs):
        return {'any': {'service': [address] }}

    return {'service_discovery': sd, 'protocol_discovery': pd}
