import app_logging
import cert
import crawl
import launch_cert_server
import model.crawl
import opener
from sessctx import SessionContext

logger = app_logging.create_logger()

COOKIE_FILE_PATH = 'cookie.txt'
DATABASE_PATH = 'db/crawl.db'


def main():
    app_logging.set_level(app_logging.INFO)

    logger.info('main')
    lcm = cert.SocketLoginCertManager(launch_cert_server.HOST, launch_cert_server.PORT)

    create_new_session = input('new session [y/n] > ').lower() == 'y'
    logger.info(f'{create_new_session=}')

    with opener.ManabaURLOpener(
            cookie_file_name=COOKIE_FILE_PATH
    ) as url_opener:
        url_opener.login(lcm)

        manaba_crawler = crawl.ManabaCrawler(
            session_context=SessionContext.create_instance(
                DATABASE_PATH,
                model.crawl.SQLCrawlerDataModelBase
            ),
            url_opener=url_opener
        )

        if create_new_session:
            manaba_crawler.initialize_by_period(
                period=crawl.ManabaCrawler.PERIOD_ALL
            )

        manaba_crawler.crawl(resume_state=manaba_crawler.RESUME_OLDEST)


if __name__ == '__main__':
    main()
