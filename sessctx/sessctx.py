import collections
import contextlib
import hashlib
from typing import Iterable, NamedTuple, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

import app_logging


class CascadingParameterEvaluator:
    UNSPECIFIED = object()

    def __init__(self, dct: Union[dict, NamedTuple]):
        if isinstance(dct, dict):
            values = collections.namedtuple(f'_ParserArgs_{id(self)}', dct.keys())(**dct)
        else:
            values = dct
        self.__values = values

    @classmethod
    def from_names(cls, *names, **names_with_defaults):
        names = set(names) | set(names_with_defaults.keys())
        dct = {name: names_with_defaults.get(name, cls.UNSPECIFIED) for name in names}
        return cls(dct)

    def __copy(self):
        return type(self)(self.__values)

    def _inplace_update(self, **kwargs):
        # noinspection PyProtectedMember
        invalid_keys = {k for k in kwargs if k not in self.__values._fields}
        if invalid_keys:
            raise ValueError('invalid keys', invalid_keys)

        # noinspection PyProtectedMember,PyUnresolvedReferences
        self.__values = self.__values._replace(**kwargs)
        return self

    def update(self, **kwargs):
        return self.__copy()._inplace_update(**kwargs)

    def parse(self):
        # noinspection PyProtectedMember,PyUnresolvedReferences
        dct = self.__values._asdict()

        empty_keys = {k for k, v in dct.items() if v is self.UNSPECIFIED}
        if empty_keys:
            raise ValueError('empty keys exist', empty_keys)

        return dct


class SessionContext:
    logger = app_logging.create_logger()

    _parameter_parser = CascadingParameterEvaluator.from_names(
        do_commit=True
    )

    def __init__(self, session_class, name=None, **kwargs):
        self.__session_class = session_class
        self.__name = name
        self.__params = self._parameter_parser.update(**kwargs)

    def _create_raw_session(self) -> Session:
        return self.__session_class()

    @staticmethod
    def _create_session_index(session: Session):
        session_index = hashlib.sha3_256(str(session).encode('utf-8')).hexdigest()[-8:]
        session_index = f'0x{session_index.upper()}'
        return session_index

    @contextlib.contextmanager
    def __call__(self, **kwargs) -> Iterable[Session]:
        params = self.__params.update(**kwargs).parse()

        session = self._create_raw_session()
        session_index = self._create_session_index(session)

        def debug(msg):
            self.logger.debug(f'session {self.__name}#{session_index} {msg}')

        debug(f'with {params} CREATED')
        try:
            yield session
        except Exception as e:
            session.rollback()
            debug(f'ROLLED BACK due to {e}')
            raise
        else:
            if params['do_commit']:
                session.commit()
                debug(f'COMMITTED')
        finally:
            session.close()
            debug(f'CLOSED')


class SessionContextGeneratorBase:
    logger = app_logging.create_logger()

    def __init__(self, engine: Engine):
        self.__engine = engine

    @classmethod
    def from_url_and_base(cls, url: str, base):
        cls.logger.info(f'session context created: {url=}, {base=}')
        engine: Engine = create_engine(url)
        base.metadata.create_all(engine)
        return cls(engine)

    def create_context(self, **kwargs):
        SessionClass = sessionmaker(self.__engine)
        return SessionContext(SessionClass, name=self.__engine.url, **kwargs)


class LocalSessionContextGenerator(SessionContextGeneratorBase):
    URL_FORMAT: str = None

    @classmethod
    def from_path_and_base(cls, path: str, base):
        url = cls.URL_FORMAT.format(path=path)
        return cls.from_url_and_base(url, base)


class RemoteSessionContextGenerator(SessionContextGeneratorBase):
    URL_FORMAT: str = None

    @classmethod
    def from_path_and_base(cls, user: str, pw: str, host: str, path: str, base):
        url = cls.URL_FORMAT.format(user=user, pw=pw, host=host, path=path)
        return cls.from_url_and_base(url, base)
