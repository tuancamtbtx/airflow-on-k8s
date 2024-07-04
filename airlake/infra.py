import contextlib
import functools
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import configuration

from airlake import resolve_root


TEAM_CONF = os.getenv("AIRLOCK_TEAM_CONF") or  resolve_root("conf", "team.yaml")


def mysql_engine():
    MYSQL_URL = os.environ.get("AIRLOCK_MYSQL_URI") or configuration.conf.get(
        "core",
        "sql_alchemy_conn",
    )
    return create_engine(MYSQL_URL, pool_size=4, pool_recycle=3600)


@contextlib.contextmanager
def create_session(engine):
    """
    Contextmanager that will create and teardown a session.
    """
    session = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class LazySessionInit:
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        self._sqlalchemy_engine = None

    @property
    def engine(self):

        if not self._sqlalchemy_engine:
            self._sqlalchemy_engine = mysql_engine()
        return self._sqlalchemy_engine

    def __call__(self, *args, **kwargs):
        with create_session(self.engine) as session:
            kwargs = {**kwargs, **{"session": session}}
            return self.func(*args, **kwargs)

    def __get__(self, instance, instancetype):
        """Implement the descriptor protocol to make decorating instance
        method possible.
        """
        # Return a partial function with the first argument is the instance
        #   of the class decorated.
        return functools.partial(self.__call__, instance)


provide_session = LazySessionInit
