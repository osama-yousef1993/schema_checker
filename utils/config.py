from os import getenv

from dotenv.main import load_dotenv

load_dotenv()

LOG_FORMAT = '%(module)s - %(levelname)-8s: %(message)s'

#
#
# ---------- database connection ----------
#
DB_DATABASE = getenv('DB_DATABASE')
DB_USERNAME = getenv('DB_USERNAME')
DB_PASSWORD = getenv('DB_PASSWORD')
DB_HOST = getenv('DB_HOST')
DB_PORT = getenv('DB_PORT')
