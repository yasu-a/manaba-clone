from model import SQLDataModelMixin, SQLDataModelBase
from model.session_util import SQLDataModelDuplicationFinderMixin


class SQLDownloaderModelBase(
    SQLDataModelBase,
    SQLDataModelMixin,
    SQLDataModelDuplicationFinderMixin
):
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
