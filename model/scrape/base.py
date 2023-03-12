from sqlalchemy.ext.declarative import declarative_base

SQLScraperDataModelBase = declarative_base()


# TODO: abstract scraper model classes, whose methods operate task_entry, by this mix-in
class TaskEntryHandlerMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
