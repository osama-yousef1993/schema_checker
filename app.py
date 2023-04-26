import time
from datetime import datetime

from handlers.process import DatabaseCheckHandler
from helpers.date import DateHelper
from utils.log import logger


def main():
    try:
        start_time = time.time()
        logger.info(
            f'Database Checker Schema started at {datetime.now()}')
        database_handler = DatabaseCheckHandler()
        logger.info("Start Table Schema Check process")
        database_handler.tables_schema()
        logger.info("Start Function And Procedure Schema Check process")
        database_handler.function_procedure_schema()
        del database_handler
        end_time = datetime.now()
        consumed_time = DateHelper.consumed_time(start_time)
        logger.info(
            f'Database Checker :: finished successfully at {end_time}, Total execution time {consumed_time}')
    except Exception as e:
        consumed_time = DateHelper.consumed_time(start_time)
        logger.info(
            f'Database Checker failed at {datetime.now()}. Total execution time {consumed_time} ', {e})


if __name__ == '__main__':
    main()
