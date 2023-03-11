import re
import urllib.error
import urllib.parse
from typing import Optional, Callable, Union, NamedTuple

import app_logging

__all__ = 'MappedURL', 'URLMapperEntry', 'URLMapper', 'ManabaURLMapper'


class MappedURL(NamedTuple):
    url: str
    mapper_name: str

    def __eq__(self, other: 'MappedURL'):
        if not isinstance(other, MappedURL):
            return False
        return self.url == other.url

    def __hash__(self):
        return hash(self.url)


URLMapping = Callable[[str, urllib.parse.ParseResult], Union[str, urllib.parse.ParseResult]]


class URLMapperEntry(NamedTuple):
    name: str
    domain: str
    path_pattern: str
    mappings: list[URLMapping]

    def match_url(self, url_components: urllib.parse.ParseResult) -> bool:
        domain, path = url_components.netloc, url_components.path

        if domain != self.domain:
            return False
        if not re.fullmatch(self.path_pattern, path):
            return False
        return True

    @staticmethod
    def __normalize_mapping_result(mapping_result):
        if isinstance(mapping_result, urllib.parse.ParseResult):
            mapping_result = urllib.parse.urlunparse(mapping_result)
        if not isinstance(mapping_result, str):
            raise ValueError('invalid type of value returned from mapper')
        return mapping_result

    def apply(self, source_url: str) -> MappedURL:
        url = source_url
        for mapping in self.mappings:
            url_component = urllib.parse.urlparse(url)
            mapping_result = mapping(url, url_component)
            url = self.__normalize_mapping_result(mapping_result)
        return MappedURL(
            url=url,
            mapper_name=self.name
        )


class URLMapper:
    logger = app_logging.create_logger()

    @classmethod
    def _mappers(cls) -> list[URLMapperEntry]:
        raise NotImplementedError()

    @classmethod
    def __find_mapper_entry(cls, url: str) -> Optional[URLMapperEntry]:
        url_components = urllib.parse.urlparse(url)
        for mapper_entry in cls._mappers():
            if mapper_entry.match_url(url_components):
                return mapper_entry
        return None

    @classmethod
    def map(cls, url: str) -> Optional[MappedURL]:
        mapper_entry = cls.__find_mapper_entry(url)
        if mapper_entry is None:
            cls.logger.debug(f'mapper filtered: {url!r}')
            return None
        mapped_url = mapper_entry.apply(url)
        cls.logger.debug(f'mapper mapped: {url!r} -> {mapped_url!r}')
        return mapped_url


# noinspection PyUnusedLocal
class ManabaURLMapper(URLMapper):
    # TODO: move into proper method local
    @staticmethod
    def course_list_mapper(url: str, parse_result: urllib.parse.ParseResult):
        query_mapping = dict(urllib.parse.parse_qsl(parse_result.query))
        query_mapping['chglistformat'] = 'list'
        new_parse_result = parse_result._replace(query=urllib.parse.urlencode(query_mapping))
        return new_parse_result

    # TODO: move into proper method local
    @staticmethod
    def normalize_start_and_page_len_query(url: str, parse_result: urllib.parse.ParseResult):
        query_mapping = dict(urllib.parse.parse_qsl(parse_result.query))
        if 'start' in query_mapping and 'pagelen' in query_mapping:
            query_mapping['start'] = '1'
            # TODO: iterate page requests to retrieve 100 or more items
            query_mapping['pagelen'] = '100'
        new_parse_result = parse_result._replace(query=urllib.parse.urlencode(query_mapping))
        return new_parse_result

    # TODO: move into proper method local
    @staticmethod
    def remove_header_fragment(url: str, parse_result: urllib.parse.ParseResult):
        new_parse_result = parse_result._replace(fragment=None)
        return new_parse_result

    @classmethod
    def _mappers(cls) -> list[URLMapperEntry]:
        return [
            URLMapperEntry(
                name='course_list',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/home_(_[a-z]+)?',
                mappings=[
                    cls.course_list_mapper
                ]
            ),
            URLMapperEntry(
                name='course',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/course_\d+',
                mappings=[]
            ),
            URLMapperEntry(
                name='course_news_list',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/course_\d+_news',
                mappings=[
                    cls.normalize_start_and_page_len_query
                ]
            ),
            URLMapperEntry(
                name='course_news',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/course_\d+_news_\d+',
                mappings=[]
            ),
            URLMapperEntry(
                name='course_contents_list',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/course_\d+_page',
                mappings=[
                    cls.remove_header_fragment
                ]
            ),
            URLMapperEntry(
                name='course_contents_page_list',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/page_\d+c\d+',
                mappings=[
                    cls.remove_header_fragment
                ]
            ),
            URLMapperEntry(
                name='course_contents_page',
                domain='room.chuo-u.ac.jp',
                path_pattern=r'/ct/page_\d+c\d+_\d+',
                mappings=[
                    cls.remove_header_fragment
                ]
            )
        ]
