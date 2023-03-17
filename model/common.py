import datetime
import re

from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm.exc import DetachedInstanceError

import app_logging

SQLDataModelBase = declarative_base()

__all__ = 'SQLDataModelBase', 'SQLDataModelMixin', 'create_timestamp'


class SQLDataModelRepresentativeMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def iter_attributes(self):
        mapper = sqlalchemy_inspect(type(self))
        for k in mapper.attrs.keys():
            if not k.startswith('_'):
                try:
                    attr = getattr(self, k)
                except DetachedInstanceError:
                    attr = '<session not attached>'
                yield k, attr

    @staticmethod
    def __map_repr_value(value):
        if isinstance(value, (str, bytes)):
            if len(value) >= 128:
                value = f'<{type(value).__name__} of length {len(value)}>'
        return value

    def __as_dict(self, history=None):
        history = history or tuple()
        if self in history:
            return '<described in ancestors>'
        new_history = history + (self,)

        dct = {}
        for name, attr in self.iter_attributes():
            if isinstance(attr, list):
                lst = []
                for sub_repr_obj in attr:
                    sub_repr_obj = sub_repr_obj.__as_dict(history=new_history)
                    lst.append(sub_repr_obj)
                attr = lst
            elif isinstance(attr, SQLDataModelRepresentativeMixin):
                attr = attr.__as_dict(history=new_history)
            else:
                attr = self.__map_repr_value(attr)
            dct[name] = attr
        dct['__name__'] = type(self).__name__

        return dct

    def as_dict(self):
        return self.__as_dict()

    def __repr__(self):
        mapper = sqlalchemy_inspect(type(self))
        try:
            dct = {k: getattr(self, k) for k in mapper.attrs.keys() if not k.startswith('_')}
        except DetachedInstanceError:
            return f'{type(self).__name__}(<session not attached>)'
        else:
            return f'{type(self).__name__}(' \
                + ', '.join(f'{k}={self.__map_repr_value(v)!r}' for k, v in dct.items()) + ')'


class SQLDataModelTableNameMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def __camel_to_snake(name):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    # noinspection PyMethodParameters
    @declared_attr
    def __tablename__(cls):
        return cls.__camel_to_snake(cls.__name__)


# TODO: define abstract model base
class SQLDataModelMixin(
    SQLDataModelRepresentativeMixin,
    SQLDataModelTableNameMixin
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # noinspection PyMethodParameters
    @declared_attr
    def logger(cls):
        # noinspection PyTypeChecker
        return app_logging.create_logger(cls=cls)


def create_timestamp() -> datetime.datetime:
    return datetime.datetime.now()
