from typing import TypeVar

from sqlalchemy.ext.declarative import declarative_base

T = TypeVar('T')

SQLScraperDataModelBase = declarative_base()
