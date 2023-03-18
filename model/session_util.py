from typing import Optional, Any
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from .common import SQLDataModelBase


class SQLDataModelDuplicationFinderMixin:
    @classmethod
    def find_duplication(
            cls,
            session: Session,
            *,
            values: dict[str, Any]
    ) -> Optional['SQLDataModelBase']:
        query = session.query(cls)
        for name, value in values.items():
            attribute = getattr(cls, name)
            query = query.where(attribute == value)
        dup_entry = query.first()
        return dup_entry
