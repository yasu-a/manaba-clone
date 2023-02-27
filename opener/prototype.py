import urllib.request
from typing import Union

RequestLike = Union[str, urllib.request.Request]


def extract_url_from_request_like(url_or_req: RequestLike) -> str:
    if isinstance(url_or_req, urllib.request.Request):
        url = url_or_req.full_url
    else:
        url = url_or_req
    if not isinstance(url, str):
        raise TypeError('invalid type of \'url_or_req\'', url_or_req, type(url_or_req))
    return url


class URLOpenerPrototype:
    RESPONSE_ENCODING = ...

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_header(self) -> dict:
        ...

    def urlopen(self, url_or_req: Union[str, urllib.request.Request]):
        ...

    def _enter_handler(self) -> None:
        pass

    def _exit_handler(self) -> None:
        pass
