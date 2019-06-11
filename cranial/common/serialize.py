import io
import tarfile
import gzip

def dir2bytes(dirname:str) -> bytes:
    """Make an in-memory gzip'd tar archive from a directory with minimal copying.

    # Test below effectively copies current dir to /tmp/doctest.

    >>> from tempfile import mkstemp, mkdtemp
    >>> dir = mkdtemp()
    >>> f = mkstemp(dir=dir)
    >>> bytes2dir(dir2bytes(dir), '/tmp/doctest')
    """

    with io.BytesIO() as buf:
        with tarfile.TarFile(mode='w', fileobj=buf) as tar:
            tar.add(dirname)
            outbytes = gzip.compress(buf.getbuffer())
    return outbytes


def bytes2dir(tarbytes: bytes, dirname = '.') -> None:
    """Take the bytes that make-up an gzip'd tar archive and decompress to disk."""
    fileobj = io.BytesIO(tarbytes)
    with tarfile.TarFile(fileobj=io.BytesIO(gzip.decompress(tarbytes))) as tar:
        tar.extractall(dirname)
