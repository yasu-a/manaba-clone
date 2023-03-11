from functools import cached_property
from functools import cached_property
from typing import Any

import bs4


class SoupParser:
    def __init__(self, soup: bs4.BeautifulSoup):
        self.__soup = soup

    @property
    def _soup(self):
        return self.__soup

    @classmethod
    def from_html(cls, html: str) -> 'SoupParser':
        soup = bs4.BeautifulSoup(html, features='lxml')
        return cls(soup)

    @cached_property
    def __properties(self) -> dict[str, Any]:
        def iter_class_properties(cls):
            for name in dir(cls):
                obj = getattr(cls, name)
                if isinstance(obj, property):
                    yield name, obj

        property_iterator = iter_class_properties(type(self))
        properties = {
            name: getattr(self, name)
            for name, obj in property_iterator
            if not name.startswith('_')
        }
        return properties

    def extract_properties(self, *names: str) -> dict[str, Any]:
        return {
            name: value
            for name, value in self.__properties.items()
            if name in names
        }
