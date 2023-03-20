import os
from pprint import pformat

import app_logging

logger = app_logging.create_logger()

_handlers = {}


def handler(name):
    def decorator(func):
        _handlers[name] = func
        return func

    return decorator


@handler('MANABA_CLONE_DEBUG')
def handler_debug(value):
    # noinspection PyBroadException
    try:
        value = int(value)
    except:
        value = {'TRUE': 1, 'FALSE': 0}[value.upper()]
    else:
        value = bool(value)
    return value


@handler('MANABA_CLONE_CRAWLER_SLEEP')
def handler_crawler_sleep(value):
    value = int(value)
    if get('MANABA_CLONE_DEBUG'):
        return 2
    else:
        return value


@handler('MANABA_CLONE_DOWNLOADER_SLEEP')
def handler_crawler_sleep(value):
    value = int(value)
    if get('MANABA_CLONE_DEBUG'):
        return 2
    else:
        return value


def setup():
    dct = {}

    invalid_keys = []
    for k in _handlers:
        v = os.environ.get(k)
        if v is None:
            invalid_keys.append(k)
        dct[k] = v

    if invalid_keys:
        raise ValueError('environment variables unset', invalid_keys)

    logger.info(f'ENV SETUP\n{pformat(dct)}')

    return dct


_env_vars = setup()


def get(key):
    if key not in _env_vars:
        raise ValueError('undefined environment variable key')
    value = _env_vars[key]
    value = _handlers[key](value=value)
    return value
