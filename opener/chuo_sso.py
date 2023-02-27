import urllib.parse
import urllib.request
from typing import Optional

from bs4 import BeautifulSoup

import app_logging
from cert import LoginCertManager
from .prototype import URLOpenerPrototype

logger = app_logging.create_logger()


class URLOpenerChuoSSOLoginMixin(URLOpenerPrototype):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__login_uid: Optional[str] = None

    LOGIN_CHECK_URL = 'https://room.chuo-u.ac.jp/ct/home'

    def check_login(self):
        with self.urlopen(self.LOGIN_CHECK_URL) as res:
            content = res.read().decode('utf-8')
            soup = BeautifulSoup(content, 'lxml')

            elm_title = soup.find('title')
            if elm_title.text == '中央大学 manaba - home':
                return None

            return res.url

    def urlopen_submit(self, soup: BeautifulSoup = None, payload=None, url=None):
        action_url = url

        if payload is None:
            elm_form = soup.find('form')
            action_url = action_url or elm_form.attrs['action']

            elm_input_lst = elm_form.find_all('input')

            payload = {}
            for elm_input in elm_input_lst:
                name = elm_input.attrs.get('name')
                value = elm_input.attrs.get('value')

                if name is None or value is None:
                    continue

                payload[name] = value

        if isinstance(payload, dict):
            payload = urllib.parse.urlencode(payload).encode('utf-8')

        req = urllib.request.Request(
            url=action_url,
            method='POST',
            data=payload,
            headers=self.create_header()
        )
        return self.urlopen(req)

    def login(self, lcm: LoginCertManager):
        uid, pw = lcm.request('chuo-sso')

        logger.info(f'login with {uid=}')

        redirect_url = self.check_login()

        if redirect_url is None:
            logger.info('already certified')
            return

        logger.info('certification required')

        with self.urlopen(redirect_url) as res:
            content = res.read().decode('utf-8')
            soup = BeautifulSoup(content, 'lxml')

            back = soup.select_one('input[name="back"]').attrs['value']
            sess_id = soup.select_one('input#sessid').attrs['value']

            payload = {
                'dummy': '',
                'username': uid,
                'password': pw,
                'op': 'login',
                'back': back,
                'sessid': sess_id
            }

            with self.urlopen_submit(
                    payload=payload,
                    url=redirect_url
            ):
                pass

        redirect_url = self.check_login()

        if redirect_url is None:
            self.__login_uid = uid
            return

        parse_result = urllib.parse.urlparse(redirect_url)
        if parse_result.netloc == 'gakunin-idp.c.chuo-u.ac.jp' \
                and parse_result.path == '/pub/login.cgi':
            raise ValueError('incorrect cert')

        with self.urlopen(redirect_url) as res:
            content = res.read().decode('utf-8')
            soup = BeautifulSoup(content, 'lxml')
            with self.urlopen_submit(soup):
                pass

        if self.check_login() is None:
            self.__login_uid = uid
            return

        raise ValueError('login failed')

    LOGOUT_URL = 'https://room.chuo-u.ac.jp/ct/logout'

    def logout(self):
        if self.__login_uid is None:
            return

        logger.info(f'logout of login_uid={self.__login_uid}')
        with self.urlopen(self.LOGOUT_URL):
            pass
        self.__login_uid = None

    def _exit_handler(self):
        self.logout()
        super()._exit_handler()
