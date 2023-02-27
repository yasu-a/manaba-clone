import json

from .cert import LoginCertManager, LoginCertPair


class JsonLoginCertManager(LoginCertManager):
    def __init__(self, json_data):
        self.__json_data = json_data

    def request(self, domain: str) -> LoginCertPair:
        cert_dict = self.__json_data.get(domain)
        if cert_dict is None:
            raise ValueError('invalid domain for cert request', domain)
        return LoginCertPair(uid=cert_dict['uid'], password=cert_dict['pw'])

    @classmethod
    def from_path(cls, path):
        with open(path, 'r') as f:
            json_data = json.load(f)
        return cls(json_data=json_data)
