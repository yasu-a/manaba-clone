from abc import abstractmethod
from typing import Optional, Any

from sqlalchemy.orm import Session

import model.crawl
from model import SQLDataModelMixin, SQLDataModelBase
from .soup_parser import SoupParser


class ParentModelEntries:
    def __init__(self, *entries: 'SQLScraperModelBase'):
        self.__entries = entries

    def add(self, entry: 'SQLScraperModelBase'):
        new_entries = self.__entries + (entry,)
        return ParentModelEntries(*new_entries)

    def __getitem__(self, model_class_name: str) -> 'SQLScraperModelBase':
        for entry in reversed(self.__entries):
            if type(entry).__name__ == model_class_name:
                return entry
        entries = [type(entry).__name__ for entry in self.__entries]
        raise ValueError(f'no entry with {model_class_name=} in ancestors: {entries=}')


class SQLScraperModelBase(SQLDataModelBase, SQLDataModelMixin):
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    @abstractmethod
    def _soup_parser(cls) -> type[SoupParser]:
        raise NotImplementedError()

    @classmethod
    def find_duplication(
            cls,
            session: Session,
            *,
            values: dict[str, Any]
    ) -> Optional['SQLScraperModelBase']:
        query = session.query(cls)
        for name, value in values.items():
            attribute = getattr(cls, name)
            query = query.where(attribute == value)
        dup_entry = query.first()
        return dup_entry

    @classmethod
    def get_dup_entry(
            cls,
            session: Session,
            *,
            task_entry: model.crawl.Task
    ) -> Optional['SQLScraperModelBase']:
        dup_entry = cls.find_duplication(
            session,
            values=dict(
                timestamp=task_entry.timestamp,
                url=task_entry.lookup.url
            )
        )

        return dup_entry

    @classmethod
    @abstractmethod
    def _create_entry_from_task_entry(
            cls: type['SQLScraperModelBase'],
            *,
            task_entry: model.crawl.Task,
            soup_parser: SoupParser
    ) -> 'SQLScraperModelBase':
        raise NotImplementedError()

    @classmethod
    def from_task_entry(
            cls: type['SQLScraperModelBase'],
            *,
            task_entry: model.crawl.Task
    ) -> Optional['SQLScraperModelBase']:
        if task_entry.page.content is None:
            return None
        soup_parser = cls._soup_parser().from_html(task_entry.page.content)

        entry = cls._create_entry_from_task_entry(
            task_entry=task_entry,
            soup_parser=soup_parser
        )

        return entry

    @abstractmethod
    def _set_parent_model_entry(self, parent_model_entry: ParentModelEntries):
        raise NotImplementedError()

    @classmethod
    def insert_from_task_entry(
            cls,
            session: Session,
            *,
            task_entry: model.crawl.Task,
            parent_model_entries: ParentModelEntries
    ) -> Optional['SQLScraperModelBase']:
        dup_entry = cls.get_dup_entry(
            session,
            task_entry=task_entry
        )
        if dup_entry is not None:
            return dup_entry

        entry = cls.from_task_entry(
            task_entry=task_entry
        )

        if entry is None:
            return None

        entry._set_parent_model_entry(parent_model_entries)

        session.add(entry)

        return entry
