# from sqlalchemy import create_engine
# from sqlalchemy.engine import Engine
# from sqlalchemy.orm import sessionmaker, Session
#
# import app_logging
# import cert
# import launch_cert_server
# import model.scrape
# import model.crawl
# import opener
# import scrape
# from sessctx import SessionContext
#
# logger = app_logging.create_logger()
#
# COOKIE_FILE_PATH = 'cookie.txt'
# DATABASE_PATH = 'scrape.db'
#
# crawler_session_context = SessionContext.create_instance(
#     DATABASE_PATH,
#     model.crawl.SQLCrawlerDataModelBase
# )
#
# scraper_session_context = SessionContext.create_instance(
#     DATABASE_PATH,
#     model.scrape.SQLScraperDataModelBase
# )
#
#
# def main():
#     logger.info('main')
#
#     mnb = scrape.ManabaScraper(url_opener)
#     logger.info('iterating courses')
#     for course_field \
#             in mnb.iter_course_entries(period=scrape.CourseListPeriod.ALL):
#         logger.info(f'course retrieved {course_field}')
#
#
# if __name__ == '__main__':
#     main()
