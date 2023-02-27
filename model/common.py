import datetime
import re
from typing import Iterable, TypeVar, Type, Optional, Callable

from sqlalchemy import inspect as sqlalchemy_inspect, exists
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import DetachedInstanceError

import app_logging
from persistent_hash import persistent_hash

T = TypeVar('T')

__all__ = 'SQLDataModelMixin', 'create_timestamp', 'create_model_parameters'


# TODO: define abstract model base
class SQLDataModelMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def __map_repr_value(value):
        if isinstance(value, str):
            if len(value) > 128:
                return f'<string with {len(value)}>'
        return value

    def __repr__(self):
        mapper = sqlalchemy_inspect(type(self))
        try:
            dct = {k: getattr(self, k) for k in mapper.attrs.keys() if not k.startswith('_')}
        except DetachedInstanceError:
            return f'{type(self).__name__}(<session not attached>)'
        else:
            return f'{type(self).__name__}(' \
                + ', '.join(f'{k}={self.__map_repr_value(v)!r}' for k, v in dct.items()) + ')'

    @classmethod
    def _filter_predicate(
            cls,
            keys: Iterable[str],
            values: dict[str, object]
    ):
        def predicate():
            result = None
            for key in keys:
                field = getattr(cls, key)
                value = field == values[key]
                if result is None:
                    result = value
                else:
                    result &= value
            return result

        return predicate

    @classmethod
    def _get_default(
            cls,
            session: Session,
            *,
            data: dict[str, object],
            unique_keys: list[str] = None,
            key: str,
            default_producer: Callable[[], object]
    ):
        unique_keys = unique_keys or list(data.keys())

        predicate = cls._filter_predicate(
            keys=unique_keys,
            values=data
        )

        logger_msg = [f'get_default: ', f' {data=!r}, {unique_keys=!r}, {key=!r}']

        entry = session.query(cls).filter(predicate()).first()

        if entry is None:
            logger_msg.insert(1, 'DEFAULT PRODUCED')
            cls.logger.debug(''.join(logger_msg))
            new_data = dict(data)
            new_data[key] = default_producer()
            entry = cls(**new_data)
            session.add(entry)
        else:
            logger_msg.insert(1, 'EXISTING RETURNED')
            cls.logger.debug(''.join(logger_msg))

        return getattr(entry, key)

    @classmethod
    def _get_one(
            cls: Type[T],
            session: Session,
            *,
            data: dict[str, object]
    ) -> Optional[T]:
        logger_msg = [f'get_one: ', f' {data=!r}']

        def predicate():
            result = None
            for data_key, data_value in data.items():
                field = getattr(cls, data_key)
                value = field == data_value
                if result is None:
                    result = value
                else:
                    result &= value
            return result

        entries = session.query(cls).filter(predicate()).all()
        entry_count = len(entries)

        if entry_count > 2:
            raise ValueError('entry contains multiple items')
        elif entry_count == 1:
            logger_msg.insert(1, 'HIT')
            cls.logger.debug(''.join(logger_msg))
            first_entry = entries[0]
            return first_entry
        else:
            logger_msg.insert(1, 'MISS')
            cls.logger.debug(''.join(logger_msg))
            return None

    @classmethod
    def _add_if_not_exists(
            cls: Type[T],
            session: Session,
            *,
            data: dict[str, object],
            unique_keys: list[str] = None
    ) -> Optional[T]:
        unique_keys = unique_keys or list(data.keys())

        predicate = cls._filter_predicate(
            keys=unique_keys,
            values=data
        )

        logger_msg = [f'add_if_not_exists: ', f' {data=!r}, {unique_keys=!r}']

        entry_exists = session.query(exists().where(predicate())).scalar()

        if entry_exists:
            logger_msg.insert(1, 'CANCELLED')
            cls.logger.debug(''.join(logger_msg))
            return None

        entry = cls(**data)
        session.add(entry)

        logger_msg.insert(1, 'EXECUTED')
        cls.logger.debug(''.join(logger_msg))
        return entry

    @staticmethod
    def __camel_to_snake(name):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    # noinspection PyMethodParameters
    @declared_attr
    def __tablename__(cls):
        return cls.__camel_to_snake(cls.__name__)

    # noinspection PyMethodParameters
    @declared_attr
    def logger(cls):
        # noinspection PyTypeChecker
        return app_logging.create_logger(cls=cls)


def create_timestamp() -> datetime.datetime:
    return datetime.datetime.now()


# TODO: remove `**` from additional_keys
def create_model_parameters(
        field: dict = None,
        *,
        field_hash: bool = False,
        timestamp: bool = False,
        keys: Iterable[str] = None,
        **additional_keys
) -> dict:
    parameters = {}

    if field is not None:
        if keys is None:
            keys = field.keys()
        parameters.update({k: field[k] for k in keys})

    if field_hash:
        parameters['hash'] = persistent_hash(field)

    if additional_keys:
        parameters.update(additional_keys)

    if timestamp:
        parameters['timestamp'] = create_timestamp()

    return parameters
