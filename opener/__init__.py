from .chuo_sso import URLOpenerChuoSSOLoginMixin
from .limiter import URLRateLimiter
from .opener import CookieURLOpenHandler, DiskURLOpenHandler, MemoryURLOpenHandler
from .util import URLOpenerUtilMethodsMixin


class ManabaURLOpener(
    CookieURLOpenHandler,
    URLOpenerChuoSSOLoginMixin,
    URLOpenerUtilMethodsMixin
):
    def __enter__(self):
        self._enter_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_handler()
        return False


class DiskURLOpener(
    DiskURLOpenHandler,
    URLOpenerUtilMethodsMixin
):
    def __enter__(self):
        self._enter_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_handler()
        return False


class MemoryURLOpener(
    MemoryURLOpenHandler,
    URLOpenerUtilMethodsMixin
):
    def __enter__(self):
        self._enter_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_handler()
        return False
