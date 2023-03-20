from sessctx import *
from .common import SQLDataModelBase

_SCG = MySQLSessionContextGenerator

if _SCG is MySQLSessionContextGenerator:

    _DATABASE_PATH = 'manaba_clone'


    def create_session_context(custom_db_path=None):
        return _SCG.from_path_and_base(
            user='user',
            pw='user01',  # TODO: store on environment variables
            host='localhost:3306',
            path=custom_db_path or _DATABASE_PATH,
            base=SQLDataModelBase
        ).create_context()

elif _SCG is SQLiteSessionContextGenerator:

    _DATABASE_PATH = 'db/database.db'


    def create_session_context(custom_db_path=None):
        # noinspection PyArgumentList
        return _SCG.from_path_and_base(
            path=custom_db_path or _DATABASE_PATH,
            base=SQLDataModelBase
        ).create_context()
