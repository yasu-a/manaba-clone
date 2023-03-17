import contextlib
import http.cookiejar
import io
import os
import urllib.parse
import urllib.request
from typing import Iterable
from typing import Union

import app_logging
from .limiter import URLRateLimiter
from .prototype import URLOpenerPrototype, RequestLike, extract_url_from_request_like

logger = app_logging.create_logger()


class URLOpenHandlerBase(URLOpenerPrototype):
    RESPONSE_ENCODING = 'utf-8'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_header(self) -> dict:
        raise NotImplementedError()

    def urlopen(self, url_or_req: RequestLike):
        raise NotImplementedError()

    def _enter_handler(self) -> None:
        pass

    def _exit_handler(self) -> None:
        pass


class CookieURLOpenHandler(URLOpenHandlerBase):
    def __init__(
            self,
            *,
            cookie_file_name: str,
            rate_limiter: URLRateLimiter = None,
            **kwargs
    ):
        super().__init__(**kwargs)

        self.__rate_limiter = rate_limiter

        self.__cookie_jar = http.cookiejar.LWPCookieJar(filename=cookie_file_name)
        self.__opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.__cookie_jar)
        )

    # noinspection PyMethodMayBeStatic
    def create_header(self) -> dict:
        default_header = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"}
        return default_header

    def urlopen(self, url_or_req: RequestLike):
        url = extract_url_from_request_like(url_or_req)
        logger.info(f'urlopen {url}')
        self.__rate_limiter.block(url)
        res = self.__opener.open(url_or_req)
        return res

    def _enter_handler(self):
        super()._enter_handler()
        try:
            self.__cookie_jar.load()
        except FileNotFoundError:
            logger.info(f'empty cookie created')
        else:
            logger.info(f'cookie loaded from {self.__cookie_jar.filename!r}')

    def _exit_handler(self):
        self.__cookie_jar.save(ignore_discard=True, ignore_expires=True)
        logger.info(f'cookie saved to {self.__cookie_jar.filename!r}')
        super()._exit_handler()


class DiskURLOpenHandler(URLOpenHandlerBase):
    def __init__(
            self,
            *,
            root_dir_path,
            **kwargs
    ):
        self.__root_dir_path = root_dir_path
        super().__init__(**kwargs)

    # noinspection PyMethodMayBeStatic
    def create_header(self) -> dict:
        return {}

    def urlopen(self, url_or_req: RequestLike):
        url = extract_url_from_request_like(url_or_req)
        logger.info(f'urlopen {url}')
        path = os.path.join(self.__root_dir_path, url)
        fs = open(path, 'rb')
        return fs

    def _enter_handler(self):
        super()._enter_handler()

    def _exit_handler(self):
        super()._exit_handler()


class MemoryURLOpenHandler(URLOpenHandlerBase):
    def __init__(
            self,
            *,
            files: dict[str, Union[str, bytes]],
            **kwargs
    ):
        if isinstance(files, dict):
            for k, v in files.items():
                if not isinstance(k, str):
                    raise TypeError('invalid type of key of files', type(k))
                if isinstance(v, str):
                    files[k] = v = v.encode(self.RESPONSE_ENCODING)
                if not isinstance(v, bytes):
                    raise TypeError('invalid type of value of files', type(k))

        self.__files = files
        super().__init__(**kwargs)

    # noinspection PyMethodMayBeStatic
    def create_header(self) -> dict:
        return {}

    def urlopen(self, url_or_req: RequestLike):
        url = extract_url_from_request_like(url_or_req)
        logger.info(f'urlopen {url}')

        @contextlib.contextmanager
        def dummy_manager() -> Iterable[io.BytesIO]:
            content = self.__files.get(url)
            if content is None:
                raise FileNotFoundError
            yield io.BytesIO(content)

        return dummy_manager()

    def _enter_handler(self):
        super()._enter_handler()

    def _exit_handler(self):
        super()._exit_handler()
