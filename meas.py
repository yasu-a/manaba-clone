import collections
import time

import numpy as np

import app_logging


class TimerReport:
    def __init__(self):
        self.__times = []
        self.__last_result = None

    __UNSPECIFIED = object()

    def append(self, value, result=__UNSPECIFIED):
        self.__times.append(value)
        if result is not self.__UNSPECIFIED:
            self.__last_result = result

    def __array(self, window_size=None, outlier=False):
        a = np.array(self.__times)
        if window_size:
            a = a[-window_size:]
        if not outlier:
            range_width = a.std() * 2  # CI=95%
            mean = a.mean()
            range_min, range_max = mean - range_width, mean + range_width
            a = a[(range_min <= a) & (a <= range_max)]
        return a

    def time_mean(self, window_size=None, outlier=True):
        a = self.__array(window_size, outlier)
        return np.mean(a)

    def time_std(self, window_size=None, outlier=True):
        a = self.__array(window_size, outlier)
        return np.std(a)

    def count(self):
        return len(self.__times)

    def outlier_count(self):
        return self.count() - len(self.__array(window_size=None, outlier=False))

    def total(self):
        return sum(self.__times)

    def __str__(self):
        return f'measured {self.count()} times' \
               f' with {self.outlier_count()} outlier(s),' \
               f' took {self.time_mean(outlier=False) * 1000:,.3f}' \
               f' (± {self.time_std(outlier=False) * 1000:,.3f}) ms.;' \
               f' last result: {self.__last_result!r}'


class Timer:
    def __init__(self, func):
        self.__func = func
        self.__reports = collections.defaultdict(TimerReport)

    @classmethod
    def __perf_counter(cls):
        return time.perf_counter_ns() * 1.0e-9

    __WINDOW_SIZE = 10
    __MIN_COUNT = 3

    def timeit(self, max_time=1, kwargs=None):
        kwargs = kwargs or {}

        if isinstance(kwargs, list):
            for single_kwargs in kwargs:
                self.timeit(max_time=max_time, kwargs=single_kwargs)
            return

        report_key = '<' + ', '.join(f'{k!s}={v!r}' for k, v in kwargs.items()) + '>'

        while True:
            app_logging.disable_logging()
            start = self.__perf_counter()
            result = self.__func(**kwargs)
            end = self.__perf_counter()
            app_logging.enable_logging()

            elapsed = end - start
            report = self.__reports[report_key]
            report.append(elapsed, result)

            if report.count() >= self.__MIN_COUNT:
                mean = report.time_mean(self.__WINDOW_SIZE)
                std = report.time_std(self.__WINDOW_SIZE)
                if mean / 100 > std:
                    break

                global_elapsed = report.total()
                if global_elapsed > max_time:
                    break

    def __str__(self):
        report_str = []
        for key, report in self.__reports.items():
            report_str.append(f'{key}\n -> {report!s}')
        report_str = [' -- Timer Report Start', *report_str, ' -- Timer Report End']
        return '\n'.join(report_str)
