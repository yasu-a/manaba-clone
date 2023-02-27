import datetime
import enum
from typing import Iterable

import opener


class CourseListPeriod(enum.Enum):
    CURRENT = ('',)
    PAST = ('_past',)
    FUTURE = ('_upcoming',)
    ALL = CURRENT + PAST + FUTURE

    @classmethod
    def default(cls):
        return cls.CURRENT


class Manaba:
    def __init__(self, url_opener: opener.ManabaURLOpener):
        self.__url_opener = url_opener

    COURSE_LIST_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/home_course{period}?chglistformat=list'

    def _open_course_list_soup(self, period_suffix):
        url = self.COURSE_LIST_URL_FORMAT.format(period=period_suffix)
        return url, self.__url_opener.urlopen_soup(url)

    def iter_course_entries(self, period: CourseListPeriod = CourseListPeriod.default) \
            -> Iterable[dict]:
        for period_suffix in period.value:
            root_url, soup = self._open_course_list_soup(period_suffix)

            def extract_field_from_table_row(tr):
                td_course_name, td_year, td_schedule, td_instructor = tr.find_all('td')
                anchor = td_course_name.select_one('.courselist-title a')
                name = anchor.text
                key = anchor.attrs['href']
                year = int(td_year.text)
                schedules = td_schedule.find('span').text
                instructors = td_instructor.text

                return dict(
                    key=key,
                    name=name,
                    year=year,
                    schedules=schedules,
                    instructors=instructors
                )

            for elm_table in soup.select('table.stdlist.courselist'):
                for elm_tr in elm_table.select('tr.courselist-c, tr.courselist-r'):
                    course_field = extract_field_from_table_row(elm_tr)
                    yield course_field

    # TODO: iterate page requests to retrieve 100 or more items
    COURSE_NEWS_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_key}_news?start=1&pagelen=100'

    def iter_course_news(self, course_key: str) -> Iterable[dict]:
        url = self.COURSE_NEWS_URL_FORMAT.format(course_key=course_key)
        soup = self.__url_opener.urlopen_soup(url)

        def extract_field_from_table_row(tr):
            title_td, sender_td, release_date_td = tr.find_all('td')
            title_anchor = title_td.find('a')
            title = title_anchor.text.strip()
            key = title_anchor.attrs['href']
            try:
                sender_name = sender_td.find_all('a')[1].text.strip()
            except IndexError:
                sender_name = sender_td.text.strip()
            release_date = datetime.datetime.strptime(
                release_date_td.text.strip(),
                '%Y-%m-%d %H:%M'
            )

            return dict(
                key=key,
                title=title,
                sender_name=sender_name,
                release_date=release_date
            )

        elm_div = soup.select_one('div.contentbody-s div.description')
        if elm_div and 'ニュースはありません' in elm_div.text:
            return

        elm_table = soup.select_one('table.stdlist')
        for elm_tr in elm_table.select('tr:not(.title)'):
            yield extract_field_from_table_row(elm_tr)

    COURSE_CONTENTS_LIST_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_key}_page'

    def iter_course_contents(self, course_key: str) -> Iterable[dict]:
        url = self.COURSE_CONTENTS_LIST_URL_FORMAT.format(course_key=course_key)
        soup = self.__url_opener.urlopen_soup(url)

        def extract_field_from_table_row(tr):
            title_td, release_date_td = tr.find_all('td')
            title_anchor = title_td.find('a')
            title = title_anchor.text.strip()
            key = title_anchor.attrs['href']
            release_date = datetime.datetime.strptime(
                release_date_td.text.split('\n')[2].strip(),
                '%Y-%m-%d %H:%M'
            )

            return dict(
                key=key,
                title=title,
                release_date=release_date
            )

        for elm_tr in soup.select('table.contentslist tr'):
            yield extract_field_from_table_row(elm_tr)

    COURSE_CONTENTS_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_contents_key}'

    def retrieve_course_contents(self, course_contents_key: str) -> dict:
        url = self.COURSE_CONTENTS_URL_FORMAT.format(course_contents_key=course_contents_key)
        soup = self.__url_opener.urlopen_soup(url)

        elm_div = soup.select('div.contentbody-left div.articletext')
        inner_html = elm_div.decode_contents(formatter='html')

        return dict(
            inner_html=inner_html
        )
