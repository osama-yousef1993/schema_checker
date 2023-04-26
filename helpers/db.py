import psycopg2

from utils.config import (DB_DATABASE, DB_HOST, DB_PASSWORD, DB_PORT,
                          DB_USERNAME)
from utils.log import logger


class DatabaseConnection():
    def db_connection(self):
        db_params = {
            "host": DB_HOST,
            "port": DB_PORT,
            "database": DB_DATABASE,
            "user": DB_USERNAME,
            "password": DB_PASSWORD
        }
        try:
            conn = psycopg2.connect(**db_params)
            logger.info("Successfully connect to database")
            return conn
        except psycopg2.OperationalError as e:
            logger.error("Unable to connect to database:", str(e))
            exit()
