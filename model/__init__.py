from .common import SQLDataModelMixin, SQLDataModelBase, create_timestamp, create_model_parameters

_DATABASE_PATH = 'db/database.db'


def create_session_context(custom_db_path=None):
    from sessctx import SessionContext
    return SessionContext.create_instance(
        custom_db_path or _DATABASE_PATH,
        SQLDataModelBase
    )
