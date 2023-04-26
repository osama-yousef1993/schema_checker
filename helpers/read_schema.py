import os

from utils.log import logger


class ReadSchema():
    def read_schema(self, path):
        if not os.path.isfile(path):
            logger.error("Schema file does not exist.")
            exit()
        with open(path, 'r') as schema_file:
            schema = schema_file.read()
        return schema
