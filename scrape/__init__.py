# import datetime
# from functools import lru_cache
# from typing import Iterable
#
# from model.crawl import Task
# from sessctx import SessionContext
#
#
# class CrawlerDBReader:
#     def __init__(
#             self,
#             crawler_session_context: SessionContext
#     ):
#         self.__sc = crawler_session_context
#
#     @lru_cache
#     def read_soup(self, value):
#         with self.__sc() as session:
#             session.query(Task.page.content)
#
#
# class ManabaScraper:
#     def __init__(
#             self,
#             crawler_session_context: SessionContext,
#             scraper_session_context: SessionContext
#     ):
#         self.__crawler_sc = crawler_session_context
#         self.__scraper_sc = scraper_session_context
#
#     def iter_course_entries(
#             self
#     ) -> Iterable[dict]:
#
#         pass
#
#     # TODO: iterate page requests to retrieve 100 or more items
#     COURSE_NEWS_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_key}_news?start=1&pagelen=100'
#
#     def iter_course_news(self, course_key: str) -> Iterable[dict]:
#         url = self.COURSE_NEWS_URL_FORMAT.format(course_key=course_key)
#         soup = self.__url_opener.urlopen_soup(url)
#
#         def extract_field_from_table_row(tr):
#             title_td, sender_td, release_date_td = tr.find_all('td')
#             title_anchor = title_td.find('a')
#             title = title_anchor.text.strip()
#             key = title_anchor.attrs['href']
#             try:
#                 sender_name = sender_td.find_all('a')[1].text.strip()
#             except IndexError:
#                 sender_name = sender_td.text.strip()
#             release_date = datetime.datetime.strptime(
#                 release_date_td.text.strip(),
#                 '%Y-%m-%d %H:%M'
#             )
#
#             return dict(
#                 key=key,
#                 title=title,
#                 sender_name=sender_name,
#                 release_date=release_date
#             )
#
#         elm_div = soup.select_one('div.contentbody-s div.description')
#         if elm_div and 'ニュースはありません' in elm_div.text:
#             return
#
#         elm_table = soup.select_one('table.stdlist')
#         for elm_tr in elm_table.select('tr:not(.title)'):
#             yield extract_field_from_table_row(elm_tr)
#
#     COURSE_CONTENTS_LIST_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_key}_page'
#
#     def iter_course_contents(self, course_key: str) -> Iterable[dict]:
#         url = self.COURSE_CONTENTS_LIST_URL_FORMAT.format(course_key=course_key)
#         soup = self.__url_opener.urlopen_soup(url)
#
#         def extract_field_from_table_row(tr):
#             title_td, release_date_td = tr.find_all('td')
#             title_anchor = title_td.find('a')
#             title = title_anchor.text.strip()
#             key = title_anchor.attrs['href']
#             release_date = datetime.datetime.strptime(
#                 release_date_td.text.split('\n')[2].strip(),
#                 '%Y-%m-%d %H:%M'
#             )
#
#             return dict(
#                 key=key,
#                 title=title,
#                 release_date=release_date
#             )
#
#         for elm_tr in soup.select('table.contentslist tr'):
#             yield extract_field_from_table_row(elm_tr)
#
#     COURSE_CONTENTS_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_contents_key}'
#
#     def retrieve_course_contents(self, course_contents_key: str) -> dict:
#         url = self.COURSE_CONTENTS_URL_FORMAT.format(course_contents_key=course_contents_key)
#         soup = self.__url_opener.urlopen_soup(url)
#
#         elm_div = soup.select('div.contentbody-left div.articletext')
#         inner_html = elm_div.decode_contents(formatter='html')
#
#         return dict(
#             inner_html=inner_html
#         )
