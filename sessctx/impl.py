from .sessctx import LocalSessionContextGenerator, RemoteSessionContextGenerator

__all__ = 'SQLiteSessionContextGenerator', 'MySQLSessionContextGenerator'


class SQLiteSessionContextGenerator(LocalSessionContextGenerator):
    URL_FORMAT = 'sqlite:///{path}?charset=utf-8'


class MySQLSessionContextGenerator(RemoteSessionContextGenerator):
    URL_FORMAT = 'mysql+mysqlconnector://{user}:{pw}@{host}/{path}'
