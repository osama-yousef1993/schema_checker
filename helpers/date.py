import time
from datetime import timedelta


class DateHelper:
    def consumed_time(start_time):
        return str(timedelta(seconds=time.time() - start_time)).split(".")[0]
