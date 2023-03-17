import inspect
import logging
import re
from logging import NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL
from typing import NamedTuple

__all__ = 'create_logger', 'set_level', 'NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'

# ANALYZER_ENABLED = True

__RE_TIMESTAMP = r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}'
__RE_LOGLEVEL = r'\[[A-Z]+\]'
__RE_NAME = r'\S+'
__RE_SEPARATOR = r'\|'
__RE_BODY = r'.*'

_RE_WHOLE_RECORD = r'\s'.join(
    map(
        '({})'.format,
        [
            __RE_TIMESTAMP,
            __RE_LOGLEVEL,
            __RE_NAME,
            __RE_SEPARATOR,
            __RE_BODY
        ]
    )
)


class LeftAdjustState:
    def __init__(self, width=0):
        self.__width = width

    def __update(self, current_width):
        if current_width > self.__width:
            self.__width = current_width
        return self.__width

    def ljust(self, s: str):
        width = self.__update(len(s))
        return s.ljust(width)


class Split(NamedTuple):
    timestamp: str
    loglevel: str
    name: str
    separator: str
    body: str

    @classmethod
    def from_string(cls, s: str):
        m = re.match(_RE_WHOLE_RECORD, s, re.DOTALL)

        return cls(*m.groups())

    def iter_split_for_lines(self):
        body_lines = self.body.split('\n')
        for body_line in body_lines:
            yield self._replace(body=body_line)

    def iter_multiline_splits(self):
        splits = list(self.iter_split_for_lines())
        for i, split in enumerate(splits):
            replacement = {}
            if len(splits) == 1:
                replacement.update(separator='─')
            else:
                if i == 0:
                    replacement.update(separator='┬')
                elif i < len(splits) - 1:
                    replacement.update(separator='│')
                else:
                    replacement.update(separator='└')
            if i != 0:
                replacement.update(timestamp=' ... ', loglevel='', name='')
            split = split._replace(**replacement)
            yield split

    def to_string(self, ljust_state: LeftAdjustState):
        header = ' '.join([self.timestamp, self.loglevel, self.name])
        separator = self.separator
        body = self.body

        header = ljust_state.ljust(header)

        return ' '.join([header, separator, body])


_global_ljust_state = LeftAdjustState()


class CustomFormatter(logging.Formatter):
    def __init__(self):
        super().__init__('%(asctime)s [%(levelname)s] %(name)s | %(message)s')

    def format(self, record: logging.LogRecord) -> str:
        s = super().format(record)
        split = Split.from_string(s)

        global _global_ljust_state
        s = '\n'.join(
            line_split.to_string(_global_ljust_state)
            for line_split in split.iter_multiline_splits()
        )

        return s


_loggers = []


def _register_logger(logger):
    _loggers.append(logger)


def set_level(level):
    global _loggers
    for logger in _loggers:
        logger.setLevel(level)


# FIXME: creating logger in a function creates duplicate log messages
def create_logger(name=None, cls: type = None) -> logging.Logger:
    module_name, class_name, function_name = None, None, None

    if name is None and cls is not None:
        module_name = cls.__module__
        class_name = cls.__qualname__

    if name is None:
        _, caller_frame_info, *_ = inspect.stack()
        caller_frame = caller_frame_info.frame
        caller_locals = caller_frame.f_locals

        if module_name is None:
            module_name = inspect.getmodule(caller_frame).__name__

        if class_name is None:
            class_name = caller_locals.get('__qualname__')
            if class_name is None:
                instance = caller_locals.get('self')
                if instance is not None:
                    class_name = type(instance).__name__
                else:
                    cls = caller_locals.get('cls')
                    if cls is not None:
                        class_name = cls.__name__

        if function_name is None:
            function_name = caller_frame_info.function

        if '__qualname__' in caller_locals:
            function_name = None
        if function_name == '<module>':
            function_name = None

        name = module_name
        if class_name is not None:
            name += f'#{class_name}'
        if function_name is not None and cls is None:
            name += f'.{function_name}'

    if name is None:
        raise RuntimeError('failed to determine logger name')

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = CustomFormatter()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    _register_logger(logger)

    return logger

# class LogAnalyzer:
#     def __init__(self):
#         pass
#
#     def analyze_body(self, timestamp, body):
#
#     def analyze(self, left, right):
#         datetime_string = re.match(r'\d+-\d+-\d+\s\d+:\d+:\d+', left).group()
#         timestamp = dateutil.parser.parse(datetime_string)
#
#         body = right[2:]
#
#         self.analyze_body(timestamp, body)


# ANALYZER_ADDRESS = 'localhost', 10001
# ANALYZER_SERVER_ADDRESS = 'localhost', 10000
#
#
# class MyHandler(BaseHTTPRequestHandler):
#     def do_GET(self):
#         pass
#
#
# log_analyzer = LogAnalyzer()
#
#
# def analyzer_callback(left, right):
#     log_analyzer.analyze_body(left, right)
#
#
# if __name__ == '__main__':
#     if ANALYZER_ENABLED:
#         httpd = HTTPServer(ANALYZER_SERVER_ADDRESS, MyHandler)
#         th = threading.Thread(target=httpd.serve_forever)
#         th.join()
