import pickle
import socket

import app_logging
from .cert import LoginCertManager, LoginCertPair

logger = app_logging.create_logger()


class SocketLoginCertManager(LoginCertManager):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port

    def request(self, domain: str) -> LoginCertPair:
        with socket.socket() as sock:
            sock.connect((self.__host, self.__port))
            sock.send(domain.encode('utf-8'))
            received = sock.recv(1024)
        cert_pair = pickle.loads(received)
        return cert_pair


def serve_forever(host, port, cert_pair_dct: dict[str, LoginCertPair]):
    logger.info(f'serve forever {host}:{port}')
    with socket.socket() as server_socket:
        server_socket.bind((host, port))
        while True:
            server_socket.listen(2)
            conn, address = server_socket.accept()
            logger.info(f'accepted {address}')
            with conn:
                domain = conn.recv(1024).decode('utf-8').strip()
                conn.send(pickle.dumps(cert_pair_dct.get(domain)))
            logger.info(f'connection closed {address}')
