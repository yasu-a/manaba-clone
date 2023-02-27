from typing import TypeVar

from sqlalchemy.ext.declarative import declarative_base

# TODO: diuse this
T = TypeVar('T')

SQLCrawlerDataModelBase = declarative_base()
