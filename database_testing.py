from pprint import pprint

from sqlalchemy.orm import aliased

import app_logging
import model
import model.crawl
import model.scrape
from testing_tools import *

logger = app_logging.create_logger()

session_context = model.create_session_context()


@test(enabled=True)
def test_1():
    with session_context(do_commit=False) as session:
        query = session.query(model.crawl.Task)
        print()
        print(query)
        print()
        result = query.first()
        pprint(result.as_dict())
        print()


@test(enabled=True)
def test_2():
    with session_context(do_commit=False) as session:
        base_task = aliased(model.crawl.Task)
        next_task = aliased(model.crawl.Task)

        query = session.query(base_task).join(
            next_task,
            base_task.url_id == next_task.back_url_id
        ).where(
            base_task.job_id == 1
        ).where(
            next_task.job_id == 1
        )

        print()
        print(query)
        print()
        for r in query.limit(10).all():
            pprint(r.as_dict())
        print(query.count())


if __name__ == '__main__':
    run_tests(list(globals().items()), run_last_only=True)
