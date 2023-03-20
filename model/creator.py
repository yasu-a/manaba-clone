from sessctx import SQLiteSessionContextGenerator
from .common import SQLDataModelBase

_DATABASE_PATH = 'db/database.db'
_SCG = SQLiteSessionContextGenerator


def create_session_context(custom_db_path=None):
    return _SCG.from_path_and_base(
        path=custom_db_path or _DATABASE_PATH,
        base=SQLDataModelBase
    ).create_context()
