import inspect
import logging
import re
from logging import NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL

__all__ = 'create_logger', 'set_level', 'NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'


# ANALYZER_ENABLED = True


class CustomFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        s = super().format(record)
        m = re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}\s[^|]*?.(?=\|)', s)
        split_index = m.end()
        left, right = s[:split_index], s[split_index:]
        s = left.ljust(60) + right

        # if ANALYZER_ENABLED:
        #     analyzer_callback(left, right)

        return s


_loggers = []


def _register_logger(logger):
    _loggers.append(logger)


def set_level(level):
    global _loggers
    for logger in _loggers:
        logger.setLevel(level)


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
    formatter = CustomFormatter('%(asctime)s [%(levelname)s] %(name)s | %(message)s')
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
