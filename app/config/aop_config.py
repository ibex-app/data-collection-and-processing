# Aspect Oriented Programming (AOP) utils
import math
import time
from random import uniform

from app.config.logging_config import log


# logging
def slf(clazz):
    """
    appends self.log to the target class
    """
    orig_init = clazz.__init__

    def __init__(self, *args, **kwargs):
        clazz.log = log
        orig_init(self, *args, **kwargs)  # Call the original __init__

    clazz.__init__ = __init__
    return clazz


# sleeping
def sleep_after(tag, pause_time=8, rang=2):
    def decorator_func(method):
        def inner(*args, **kwargs):
            res = method(*args, **kwargs)
            time_offset = uniform(-rang, rang)
            log.success(f'[{tag}] Sleeping for {math.floor((pause_time + time_offset)*100)*1.0/100} seconds.')
            time.sleep(pause_time + time_offset)
            return res
        return inner
    return decorator_func
