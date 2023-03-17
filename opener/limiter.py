import collections
import time
import urllib.parse
import urllib.request

import app_logging

logger = app_logging.create_logger()


class URLRateLimiter:
    def __init__(self, sleep=15):
        logger.info(f'initialized with {sleep=}')
        self.__sleep = sleep
        self.__last_time_blocking_finished = collections.defaultdict(float)

    @classmethod
    def __now(cls) -> float:
        return time.time()

    def block(self, url):
        domain = urllib.parse.urlparse(url).netloc

        last_time = self.__last_time_blocking_finished[domain]
        elapsed = self.__now() - last_time
        required_sleep = max(0.1, self.__sleep - elapsed)
        logger.info(f'blocking {domain!r} for {required_sleep:.3f} seconds...')
        time.sleep(required_sleep)

        self.__last_time_blocking_finished[domain] = self.__now()
