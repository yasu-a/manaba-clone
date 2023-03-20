import app_logging
import cert
import env
import launch_cert_server
import model.crawl
import opener
import worker.crawl

logger = app_logging.create_logger()

COOKIE_FILE_PATH = 'cookie.txt'


def main():
    app_logging.set_level(app_logging.DEBUG)

    logger.info('crawler main')
    lcm = cert.SocketLoginCertManager(launch_cert_server.HOST, launch_cert_server.PORT)

    session_context = model.create_session_context()

    create_new_session = input('new session [y/n] > ').lower() == 'y'
    logger.info(f'{create_new_session=}')

    with opener.ManabaURLOpener(
            cookie_file_name=COOKIE_FILE_PATH,
            rate_limiter=opener.URLRateLimiter(sleep=env.get('MANABA_CLONE_CRAWLER_SLEEP'))
    ) as url_opener:
        url_opener.login(lcm)

        manaba_crawler = worker.crawl.ManabaCrawler(
            session_context=session_context,
            url_opener=url_opener
        )

        if create_new_session:
            manaba_crawler.initialize_tasks_by_period(
                period=worker.crawl.ManabaCrawler.PERIOD_ALL
            )

        manaba_crawler.crawl(resume_state=manaba_crawler.RESUME_LATEST)


if __name__ == '__main__':
    main()
