import getpass

from cert import serve_forever, LoginCertPair

HOST, PORT = 'localhost', 9876

if __name__ == '__main__':
    uid = input('uid? > ')
    pw = getpass.getpass('pw? > ')

    cert_pair_dct = {
        "chuo-sso": LoginCertPair(uid=uid, password=pw)
    }

    serve_forever(HOST, PORT, cert_pair_dct)
