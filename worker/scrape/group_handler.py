from pprint import pformat
from typing import Callable

from sqlalchemy.orm import Session

import app_logging
import model.crawl
import model.scrape


def group_handler(*, group_name: str):
    def decorator(func):
        setattr(func, '_group_handler', {'group_name': group_name})
        return func

    return decorator


class GroupHandlerMixin:
    logger = app_logging.create_logger()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __find_group_handler(self, group_name: str) -> Callable[..., bool]:
        for name in dir(self):
            obj = getattr(self, name)
            param = getattr(obj, '_group_handler', None)
            if param is None:
                continue
            if param['group_name'] == group_name:
                return obj

    def handle_by_group_name(self, task_entry: model.crawl.Task, scraper_session: Session) -> bool:
        group_name = task_entry.lookup.group_name
        handler = self.__find_group_handler(group_name)
        if handler:
            self.logger.debug(f'ACCEPTED HANDLING {group_name}')
            handler_kwargs = dict(
                task_entry=task_entry,
                scraper_session=scraper_session
            )
            return handler(**handler_kwargs)
        else:
            self.logger.warning(f'IGNORED HANDLING {group_name}\n{pformat(task_entry.as_dict())}')
            return False
