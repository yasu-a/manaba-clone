from .sessctx import LocalSessionContextGenerator

__all__ = 'SQLiteSessionContextGenerator', 'MySQLSessionContextGenerator'


class SQLiteSessionContextGenerator(LocalSessionContextGenerator):
    URL_FORMAT = 'sqlite:///{path}?charset=utf-8'


class MySQLSessionContextGenerator(LocalSessionContextGenerator):
    URL_FORMAT = 'mysql+mysqlconnector:///{path}?charset=utf-8'
