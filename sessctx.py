import contextlib
import hashlib
from typing import Callable
from typing import Iterable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

import app_logging


# TODO: abstract duplication
class SessionContext:
    logger = app_logging.create_logger()

    def __init__(self, session_class, do_commit=None):
        self.__session_class = session_class
        self.__do_commit = do_commit

    @staticmethod
    def __eval_prioritized_values(values, *, default):
        for value in values:
            if value is None:
                continue
            return value
        return default

    @contextlib.contextmanager
    def __call__(self, *, do_commit=None) -> Iterable[Session]:
        session: Session = self.__session_class()
        session_index = hashlib.sha3_256(str(session).encode('utf-8')).hexdigest()[-8:]
        session_index = f'0x{session_index.upper()}'
        self.logger.debug(f'session {session_index} CREATED')
        try:
            yield session
        except Exception as e:
            session.rollback()
            self.logger.debug(f'session {session_index} ROLLED BACK due to {e}')
            raise
        else:
            do_commit_final = self.__eval_prioritized_values(
                [do_commit, self.__do_commit],
                default=True
            )
            if do_commit_final:
                session.commit()
                self.logger.debug(f'session {session_index} COMMITTED')
        finally:
            session.close()
            self.logger.debug(f'session {session_index} CLOSED')

    @classmethod
    def create_session_class(cls, db_path: str, base) -> Callable[..., Session]:
        engine: Engine = create_engine(f'sqlite:///{db_path}?charset=utf-8')
        base.metadata.create_all(engine)
        SessionClass = sessionmaker(engine)
        return SessionClass

    @classmethod
    def create_instance(cls, db_path: str, base, **kwargs):
        SessionClass = cls.create_session_class(db_path, base)
        cls.logger.info(f'session context created: {db_path=}, {base=}')
        return cls(SessionClass, **kwargs)
