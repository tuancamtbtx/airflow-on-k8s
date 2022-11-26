try:
    from collections.abc import Iterable as CollectionIterable
except ImportError:
    from collections import Iterable as CollectionIterable
log = logging.getLogger(__name__)

class SFTPHook():
    conn_name_attr = "sftp_conn_id"
    default_conn_name = "sftp_default"
    
    def get_conn(self):
        pass
	
