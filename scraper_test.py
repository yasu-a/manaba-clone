from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

import app_logging
import cert
import launch_cert_server
import model
import opener
import scrape

logger = app_logging.create_logger()

COOKIE_FILE_PATH = 'cookie.txt'
DATABASE_PATH = 'test.db'

engine: Engine = create_engine(f'sqlite:///{DATABASE_PATH}?charset=utf-8')

model.SQLDataModelBase.metadata.create_all(engine)

SessionClass = sessionmaker(engine)


class SessionContext:
    logger = app_logging.create_logger()

    def __init__(self, session_class: SessionClass):
        self.__session: Session = session_class()
        logger.info(f'session {self.__session!r} created')

    def __enter__(self):
        return self.__session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            logger.info(f'session {self.__session!r} committed')
            self.__session.commit()
        else:
            logger.warning(f'session {self.__session!r} rolled back')
            self.__session.rollback()
        self.__session.close()
        return False


def main():
    logger.info('main')
    lcm = cert.SocketLoginCertManager(launch_cert_server.HOST, launch_cert_server.PORT)

    with opener.ManabaURLOpener(
            cookie_file_name=COOKIE_FILE_PATH
    ) as url_opener:
        url_opener.login(lcm)

        mnb = scrape.Manaba(url_opener)
        logger.info('iterating courses')
        for course_field \
                in mnb.iter_course_entries(period=scrape.CourseListPeriod.ALL):
            logger.info(f'course retrieved {course_field}')
            with SessionContext(SessionClass) as session:
                course = model.Course.insert(
                    session,
                    course_field
                )

            for course_news_field in mnb.iter_course_news(course_field['key']):
                logger.info(f'course_news retrieved {course_news_field}')
                with SessionContext(SessionClass) as session:
                    course_news = model.CourseNews.insert(
                        session,
                        course_news_field,
                        course=course
                    )

            for course_contents_field in mnb.iter_course_contents(course_field['key']):
                logger.info(f'course_contents retrieved {course_contents_field}')
                with SessionContext(SessionClass) as session:
                    course_contents = model.CourseContents.insert(
                        session,
                        course_contents_field,
                        course=course
                    )


if __name__ == '__main__':
    main()
