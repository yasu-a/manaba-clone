import app_logging
import cert
import launch_cert_server
import model.crawl
import opener
import worker.downloader

logger = app_logging.create_logger()

# TODO: unify duplicated definitions
COOKIE_FILE_PATH = 'cookie.txt'


def main():
    app_logging.set_level(app_logging.DEBUG)

    logger.info('downloader main')
    lcm = cert.SocketLoginCertManager(launch_cert_server.HOST, launch_cert_server.PORT)

    with opener.ManabaURLOpener(
            cookie_file_name=COOKIE_FILE_PATH,
            rate_limiter=opener.URLRateLimiter(sleep=5)  # TODO: configure sleep outside of script
    ) as url_opener:
        url_opener.login(lcm)

        manaba_downloader = worker.downloader.ManabaAttachmentDownloader(
            session_context=model.create_session_context(),
            url_opener=url_opener
        )

        manaba_downloader.download_all()


if __name__ == '__main__':
    main()
