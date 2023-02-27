import contextlib
import hashlib
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

import app_logging


# TODO: abstract duplication
class SessionContext:
    logger = app_logging.create_logger()

    def __init__(self, session_class):
        self.__session_class = session_class

    @contextlib.contextmanager
    def __call__(self):
        session: Session = self.__session_class()
        session_index = hashlib.sha3_256(str(session).encode('utf-8')).hexdigest()[-8:]
        session_index = f'0x{session_index.upper()}'
        self.logger.info(f'session {session_index} CREATED')
        try:
            yield session
        except Exception as e:
            session.rollback()
            self.logger.info(f'session {session_index} ROLLED BACK due to {e}')
            raise
        else:
            session.commit()
            self.logger.info(f'session {session_index} COMMITTED')
        finally:
            session.close()
            self.logger.info(f'session {session_index} CLOSED')

    @classmethod
    def create_session_class(cls, db_path: str, base) -> Callable[..., Session]:
        engine: Engine = create_engine(f'sqlite:///{db_path}?charset=utf-8')
        base.metadata.create_all(engine)
        SessionClass = sessionmaker(engine)
        return SessionClass

    @classmethod
    def create_instance(cls, db_path: str, base):
        SessionClass = cls.create_session_class(db_path, base)
        return cls(SessionClass)
