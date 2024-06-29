try:
    from collections.abc import Iterable as CollectionIterable
except ImportError:
    from collections import Iterable as CollectionIterable

from airflow.providers.sftp.hooks.sftp import SFTPHook

log = logging.getLogger(__name__)

class CakeSFTPHook(SFTPHook):
    pass
	
