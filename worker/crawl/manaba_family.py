try:
    from .page_family import PageFamily, page_group_with_domain
except ImportError:
    from page_family import PageFamily, page_group_with_domain
import urllib.parse


class ManabaPageFamily(PageFamily):
    @staticmethod
    def course_list_mapper(url_components: urllib.parse.ParseResult):
        query_mapping = dict(urllib.parse.parse_qsl(url_components.query))
        query_mapping['chglistformat'] = 'list'
        new_components = url_components._replace(query=urllib.parse.urlencode(query_mapping))
        return new_components

    @staticmethod
    def normalize_start_and_page_len_query(url_components: urllib.parse.ParseResult):
        query_mapping = dict(urllib.parse.parse_qsl(url_components.query))
        if 'start' in query_mapping and 'pagelen' in query_mapping:
            query_mapping['start'] = '1'
            # TODO: iterate page requests to retrieve 100 or more items
            query_mapping['pagelen'] = '100'
        new_parse_result = url_components._replace(query=urllib.parse.urlencode(query_mapping))
        return new_parse_result

    @staticmethod
    def remove_header_fragment(url_components: urllib.parse.ParseResult):
        new_parse_result = url_components._replace(fragment=None)
        return new_parse_result

    with page_group_with_domain(domain='room.chuo-u.ac.jp') as manaba_page_group:
        course_list = manaba_page_group(
            path_pattern=r'/ct/home_(_[a-z]+)?',
            url_mappers=[
                course_list_mapper
            ]
        )
        course = manaba_page_group(
            path_pattern=r'/ct/course_\d+',
            parent=course_list
        )
        course_news_list = manaba_page_group(
            path_pattern=r'/ct/course_\d+_news',
            url_mappers=[
                normalize_start_and_page_len_query
            ],
            parent=course
        )
        course_news = manaba_page_group(
            path_pattern=r'/ct/course_\d+_news_\d+',
            parent=course_news_list
        )
        course_contents_list = manaba_page_group(
            path_pattern=r'/ct/course_\d+_page',
            url_mappers=[
                remove_header_fragment
            ],
            parent=course
        )
        course_contents_page_list = manaba_page_group(
            path_pattern=r'/ct/page_\d+c\d+',
            url_mappers=[
                remove_header_fragment
            ],
            parent=course_contents_list
        )
        course_contents_page = manaba_page_group(
            path_pattern=r'/ct/page_\d+c\d+_\d+',
            url_mappers=[
                remove_header_fragment
            ],
            parent=course_contents_page_list
        )


if __name__ == '__main__':
    from pprint import pprint

    pprint(
        ManabaPageFamily.apply_maps('https://room.chuo-u.ac.jp/ct/page_3969412c3427066_2686057452')
    )
