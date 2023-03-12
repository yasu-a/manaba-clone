from pprint import pformat

from sqlalchemy.orm import Session

import app_logging
import model.crawl
import model.scrape


def mapper_handler(*, mapper_name: str):
    def decorator(func):
        setattr(func, '_mapper_name_handler', {'mapper_name': mapper_name})
        return func

    return decorator


class MapperHandlerMixin:
    logger = app_logging.create_logger()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __find_mapper_name_handler(self, mapper_name: str):
        for name in dir(self):
            obj = getattr(self, name)
            param = getattr(obj, '_mapper_name_handler', None)
            if param is None:
                continue
            if param['mapper_name'] == mapper_name:
                return obj

    def handle_by_mapper_name(self, task_entry: model.crawl.Task, scraper_session: Session):
        mapper_name = task_entry.lookup.mapper_name
        handler = self.__find_mapper_name_handler(mapper_name)
        if handler:
            self.logger.debug(f'ACCEPTED HANDLING {mapper_name}')
            handler_kwargs = dict(
                task_entry=task_entry,
                scraper_session=scraper_session
            )
            handler(**handler_kwargs)
        else:
            self.logger.warning(f'IGNORED HANDLING {mapper_name}\n{pformat(task_entry.as_dict())}')
