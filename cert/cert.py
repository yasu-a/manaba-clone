import abc
import collections

LoginCertPair = collections.namedtuple('LoginCertPair', 'uid, password')


class LoginCertManager(metaclass=abc.ABCMeta):
    def request(self, domain: str) -> LoginCertPair:
        raise NotImplementedError()
