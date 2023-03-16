from sqlalchemy.orm import Session
from sqlalchemy.schema import Column, Index
from sqlalchemy.types import INTEGER, TEXT

from model import SQLDataModelMixin, SQLDataModelBase
from worker.crawl.page_family import GroupedURL
from .common import string_hash_63


# noinspection PyShadowingBuiltins
class Lookup(SQLDataModelMixin, SQLDataModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    url = Column(TEXT, unique=True)
    group_name = Column(TEXT)

    id_index = Index('id')
    url_index = Index('url')

    __UNSPECIFIED = object()

    @classmethod
    def _is_specified(cls, obj):
        return obj is not cls.__UNSPECIFIED

    @classmethod
    def _is_unspecified(cls, obj):
        return obj is cls.__UNSPECIFIED

    @classmethod
    def lookup(
            cls,
            session: Session,
            *,
            id=__UNSPECIFIED,
            url=__UNSPECIFIED
    ) -> 'Lookup':
        if (cls._is_specified(id) and cls._is_specified(url)) \
                or (cls._is_unspecified(id) and cls._is_unspecified(url)):
            raise ValueError('either \'id\' or \'url\' should be specified')

        if cls._is_specified(id):
            group_name = None
            filter_predicate = Lookup.id == id
        elif cls._is_specified(url):
            if isinstance(url, GroupedURL):
                group_name = url.group_name
                url = url.url
            else:
                group_name = None
            filter_predicate = Lookup.url == url
        else:
            assert False

        entry = session.query(Lookup).filter(
            filter_predicate
        ).first()

        if entry is not None:
            return entry

        if cls._is_specified(id):
            raise ValueError(f'unregistered {id=!r}')
        elif cls._is_specified(url):
            if group_name is None and url is not None:
                raise ValueError('new url entry must have non-null group_name')
            entry = cls(
                id=string_hash_63(url),
                url=url,
                group_name=group_name
            )
        else:
            assert False

        session.add(entry)

        return entry
