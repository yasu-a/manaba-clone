import bs4

from .prototype import RequestLike, URLOpenerPrototype


class URLOpenerUtilMethodsMixin(URLOpenerPrototype):
    def urlopen_bytes(self, url_or_req: RequestLike):
        with self.urlopen(url_or_req) as res:
            return res.read()

    def urlopen_string(self, url_or_req: RequestLike):
        return self.urlopen_bytes(url_or_req).decode(self.RESPONSE_ENCODING)

    def urlopen_string_and_soup(self, url_or_req: RequestLike):
        string = self.urlopen_string(url_or_req)
        return string, bs4.BeautifulSoup(string, features='lxml')

    # TODO: use urlopen_soup on sso-login
    def urlopen_soup(self, url_or_req: RequestLike):
        string, soup = self.urlopen_string_and_soup(url_or_req)
        return soup

    def _enter_handler(self) -> None:
        super()._enter_handler()

    def _exit_handler(self) -> None:
        super()._exit_handler()
