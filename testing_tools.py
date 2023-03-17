import app_logging

__all__ = 'test', 'run_tests'

logger = app_logging.create_logger()


def test(*, enabled: bool = False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if enabled:
                logger.info(f'test {func.__name__} start')
                result = func(*args, **kwargs)
                logger.info(f'test {func.__name__} end')
                return result

        setattr(wrapper, '_test', {})

        return wrapper

    return decorator


def list_tests(list_of_globals):
    def iter_tests():
        for name, obj in list_of_globals:
            if callable(obj):
                param = getattr(obj, '_test', None)
                if param is not None:
                    yield obj

    return list(iter_tests())


def run_tests(list_of_globals, *, run_last_only=False):
    list_of_tests = list_tests(list_of_globals)
    if run_last_only:
        list_of_tests = list_of_tests[-1:]
    for test_func in list_of_tests:
        test_func()
