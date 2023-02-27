from typing import TypeVar

from sqlalchemy.ext.declarative import declarative_base

T = TypeVar('T')

SQLScrapedDataModelBase = declarative_base()
