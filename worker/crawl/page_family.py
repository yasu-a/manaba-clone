import contextlib
import re
import urllib.error
import urllib.parse
from typing import Optional, Callable, Union, NamedTuple, Iterable

import app_logging


class GroupedURL(NamedTuple):
    url: str
    group_name: str

    def __eq__(self, other: 'GroupedURL'):
        if not isinstance(other, GroupedURL):
            return False
        return self.url == other.url

    def __hash__(self):
        return hash(self.url)

    def is_child_of(
            self,
            parent_grouped_url: 'GroupedURL',
            *,
            page_family: type['PageFamily']
    ) -> bool:
        this_group = page_family.find_page_group_by_name(self.group_name)
        if this_group is None:
            return False
        parent_group = page_family.find_page_group_by_name(parent_grouped_url.group_name)
        if parent_group is None:
            return False
        return parent_group == this_group.parent


URLMapperType \
    = Callable[[urllib.parse.ParseResult], Union[str, urllib.parse.ParseResult]]


class PageGroup(NamedTuple):
    name: str
    domain: str
    path_pattern: str
    url_mappers: list[URLMapperType]
    parent: 'PageGroup'

    def __eq__(self, other):
        if not isinstance(other, PageGroup):
            return False
        return self.domain == other.domain and self.name == other.name

    def __hash__(self):
        return hash((self.domain, self.name))

    def match_url(self, url_components: urllib.parse.ParseResult) -> bool:
        domain, path = url_components.netloc, url_components.path

        if domain != self.domain:
            return False
        if not re.fullmatch(self.path_pattern, path):
            return False
        return True

    def map(self, source_url: str) -> GroupedURL:
        if self.url_mappers:
            url_components = urllib.parse.urlparse(source_url)
            for mapper in self.url_mappers:
                mapping_result = mapper(url_components)
                if isinstance(mapping_result, str):
                    url_components = urllib.parse.urlparse(mapping_result)
            url = urllib.parse.urlunparse(url_components)
        else:
            url = source_url
        return GroupedURL(
            url=url,
            group_name=self.name
        )

    @classmethod
    def from_args(
            cls,
            *,
            name,
            domain,
            path_pattern,
            url_mappers,
            parent,
            _allow_parent_as_string=False
    ):
        e = TypeError(
            'invalid type of arg',
            dict(
                name=name,
                domain=domain,
                path_pattern=path_pattern,
                url_mappers=url_mappers,
                parent=parent
            )
        )

        if not isinstance(name, str):
            raise e
        if not isinstance(domain, str):
            raise e
        if not isinstance(path_pattern, str):
            raise e
        if not isinstance(url_mappers, list):
            raise e

        def coerce_mapper(mapper):
            if isinstance(mapper, staticmethod):
                mapper = mapper.__func__
            return mapper

        url_mappers = [coerce_mapper(mapper) for mapper in url_mappers]
        if not all(map(callable, url_mappers)):
            raise e

        if parent is not None:
            if not isinstance(parent, PageGroup):
                if _allow_parent_as_string:
                    raise e
                else:
                    if not isinstance(parent, str):
                        raise e

        return cls(
            name=name,
            domain=domain,
            path_pattern=path_pattern,
            url_mappers=url_mappers,
            parent=parent
        )


class PageGroupGeneratorParams(dict):
    pass


class PageFamilyMeta(type):
    @staticmethod
    def extract_page_groups(namespace_dct):
        def set_name(dct: dict[str, dict]) -> dict[str, dict]:
            for name, params in dct.items():
                params['name'] = name
            return dct

        def parse_parent_to_name(dct: dict[str, dict]) -> dict[str, dict]:
            for name, params in dct.items():
                # Here, params['parent'] is either dict of params itself or string
                # describes the name.
                if isinstance(params['parent'], dict):
                    params['parent'] = params['parent']['name']
            return dct

        def instantiate(dct: dict[str, dict]) -> dict[str, PageGroup]:
            new_dct = {}
            for name, params in dct.items():
                new_dct[name] = PageGroup.from_args(**params)
            return new_dct

        def resolve_parent(dct: dict[str, PageGroup]) -> dict[str, PageGroup]:
            new_dct = {}
            for name, pg in dct.items():
                # set None or replace if parent is not None
                # TODO: PageGroup parent linking which not dependent on its instance
                # noinspection PyTypeChecker
                new_dct[name] = pg.parent and pg._replace(parent=dct[pg.parent])
            return new_dct

        obj_dct = {
            name: obj
            for name, obj in namespace_dct.items()
            if isinstance(obj, PageGroupGeneratorParams)
        }
        obj_dct = set_name(obj_dct)
        obj_dct = parse_parent_to_name(obj_dct)
        pg_dct = instantiate(obj_dct)
        pg_dct = resolve_parent(pg_dct)

        namespace_dct.update(pg_dct)
        namespace_dct['_page_groups'] = pg_dct

        return namespace_dct

    def __new__(mcs, name, bases, dct):
        dct = mcs.extract_page_groups(dct)
        page_family = super().__new__(mcs, name, bases, dct)

        return page_family


class PageFamily(metaclass=PageFamilyMeta):
    logger = app_logging.create_logger()

    _page_groups: dict[str, PageGroup]  # real value generated on PageFamilyMeta.__new__

    @classmethod
    def __iter_page_groups(cls) -> Iterable[PageGroup]:
        return cls._page_groups.values()

    @classmethod
    def __find_page_group_for_url(cls, url: str) -> Optional[PageGroup]:
        url_components = urllib.parse.urlparse(url)
        for page_group in cls.__iter_page_groups():
            if page_group.match_url(url_components):
                return page_group
        return None

    @classmethod
    def find_page_group_by_name(cls, name: str) -> PageGroup:
        return cls._page_groups[name]

    @classmethod
    def apply_maps(cls, url: str) -> Optional[GroupedURL]:
        page_group = cls.__find_page_group_for_url(url)
        if page_group is None:
            cls.logger.debug(f'grouper DENIED: {url!r}')
            return None
        mapped_url = page_group.map(url)
        cls.logger.debug(f'grouper ACCEPTED: {url!r}\n -> {mapped_url!r}')
        return mapped_url


@contextlib.contextmanager
def page_group_with_domain(domain=None):
    def page_group_generator(*, path_pattern, url_mappers=None, parent=None):
        assert domain is not None
        assert path_pattern is not None
        url_mappers = url_mappers or []
        return PageGroupGeneratorParams(
            name=None,
            domain=domain,
            path_pattern=path_pattern,
            url_mappers=url_mappers,
            parent=parent
        )

    yield page_group_generator
