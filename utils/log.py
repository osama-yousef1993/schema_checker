import logging

from utils.config import LOG_FORMAT

# Add logger for app.
logger = logging.getLogger('owk-campaign-performance-sync-mediamath-service')
logger.setLevel(logging.DEBUG)

# Create the handler and set warning the level.
warning_handler = logging.StreamHandler()
warning_handler.setLevel(logging.WARNING)
warning_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# Create the handler and set info the level.
info_handler = logging.StreamHandler()
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# Create the handler and set error the level.
error_handler = logging.StreamHandler()
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# Set the handlers on the logger object.
logger.addHandler(warning_handler)
logger.addHandler(info_handler)
logger.addHandler(error_handler)
