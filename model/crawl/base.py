from model import SQLDataModelMixin, SQLDataModelBase


class SQLCrawlerModelBase(SQLDataModelBase, SQLDataModelMixin):
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
