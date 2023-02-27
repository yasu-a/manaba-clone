import datetime
import hashlib

__all__ = 'persistent_hash'


def persistent_string_hash(string):
    digest_bytes = hashlib.md5(string.encode('utf-8')).digest()
    hash_value = int.from_bytes(digest_bytes[:8], byteorder='big')
    return hash_value


def hash_handler(*classes):
    def decorator(func):
        allowable_classes = []
        checkers = []

        for cls in classes:
            try:
                isinstance(None, cls)
            except TypeError:
                checkers.append(cls)
            else:
                allowable_classes.append(cls)

        def check_type(obj):
            for allowable_class in allowable_classes:
                if isinstance(obj, allowable_class):
                    return True
            for checker in checkers:
                if checker(obj):
                    return True
            return False

        setattr(
            func,
            '_hash_handler',
            {
                'checker': check_type
            }
        )
        return func

    return decorator


@hash_handler(type(None))
def hash_none(_):
    return 1


@hash_handler(int, float, str, bytes)
def hash_constant(value):
    return persistent_string_hash(str(value))


@hash_handler(list, tuple)
def hash_ordered_set(value):
    total_hash = 17

    for item in value:
        item_hash = hash_any(item)
        total_hash = total_hash * 31 + item_hash
        total_hash %= 2 ** 63

    return total_hash


@hash_handler(set, frozenset)
def hash_unordered_set(value):
    sorted_value = sorted(value)
    return hash_ordered_set(sorted_value)


@hash_handler(dict)
def hash_mapping(value):
    sorted_entries = sorted(value.items(), key=lambda entry: entry[0])
    return hash_any(sorted_entries)


@hash_handler(datetime.datetime)
def hash_datetime_datetime(value):
    return hash_ordered_set(value.timetuple())


# https://stackoverflow.com/questions/2166818/how-to-check-if-an-object-is-an-instance-of-a-namedtuple
def is_instance_of_namedtuple(obj):
    t = type(obj)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(t, '_fields', None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


@hash_handler(is_instance_of_namedtuple)
def hash_namedtuple(value):
    # noinspection PyProtectedMember
    return hash_mapping(value._asdict())


def create_handler_lut(globals_):
    handlers = []
    for v in globals_:
        handler_params = getattr(v, '_hash_handler', None)
        if handler_params:
            params = handler_params
            handlers.append((params, v))
    return handlers


_handlers = create_handler_lut(list(globals().values()))


def hash_any(value):
    for params, handler in _handlers:
        checker = params['checker']
        if checker(value):
            return handler(value)

    handler = getattr(value, '__persistent_hash__', None)
    if handler:
        return handler()

    raise TypeError(f'persistent hash unable to hash {type(value).__name__!r}')


def persistent_hash(value):
    return hash_any(value)


if __name__ == '__main__':
    h = persistent_hash({
        '_type': 'Course',
        'course_times': [{'_type': 'CourseTime', 'day': 5, 'period': 3},
                         {'_type': 'CourseTime', 'day': 5, 'period': 4}],
        'instructor': '白井\u3000宏',
        'name': '電磁気学及演習２',
        'serial': '3428678',
        'terms': [1],
        'url': 'https://room.chuo-u.ac.jp/ct/course_3428678',
        'year': 2022
    })
    print(h)
