from requests_futures.sessions import FuturesSession

from cranial.connectors import base


class Connector(base.Connector):
    r"""Connector for getting data via HTTP. Upload not yet implemented.

    >>> stream = Connector().get('http://httpbin.org/html')
    >>> next(stream).startswith('<!DOCTYPE html>')
    True
    >>> r = Connector('http://httpbin.org').getMultiple({'rabbit': 'image/png', 'grenade': 'bytes/10'}, binary=True)
    >>> next(r['rabbit'])
    b'\x89PNG\r\n'
    """

    def __init__(self, host: str = None):
        self.session = FuturesSession()
        # Ensure host ends in '/' if given.
        self.url_host = host if host is None or host.endswith('/') else host+'/'


    def get(self, url, binary=False):
        url = url if not self.url_host else self.url_host + url
        r = self.getFuture(url).result()
        return self.toStream(r.content if binary else r.text)


    def getFuture(self, url):
        url = url if not self.url_host else self.url_host + url
        return self.session.get(url)


    def getMultiple(self, d, blocking=True, binary=False):
        result = super(Connector, self).getMultiple(d, blocking)
        if not(blocking):
            return result

        for key in result:
            r = result[key]
            result[key] = self.toStream(r.content if binary else r.text)

        return result


if __name__ == "__main__":
    import doctest
    doctest.testmod()
