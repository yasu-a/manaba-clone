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

    def handle_by_group_name(self, task_entry: model.crawl.Task, session: Session) -> bool:
        group_name = task_entry.lookup.group_name

        def print_log(state):
            self.logger.info(f'{state} HANDLING {group_name}\n{task_entry.lookup.url}')
            self.logger.debug(f'{pformat(task_entry.as_dict())=}')

        handler = self.__find_group_handler(group_name)
        if handler:
            print_log('ACCEPTED')
            handler_kwargs = dict(
                task_entry=task_entry,
                session=session
            )
            return handler(**handler_kwargs)
        else:
            print_log('IGNORED')
            return False
