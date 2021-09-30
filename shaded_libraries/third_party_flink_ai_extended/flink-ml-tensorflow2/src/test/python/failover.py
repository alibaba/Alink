import sys
import time
import traceback
import logging
import logging.config


def map_func(context):
    key = context.identity
    index = context.index
    fail_num = context.get_failed_num()
    logging.info(key + " fail num: " + str(fail_num))
    sys.stdout.flush()
    if 1 == index and fail_num < 2:
        time.sleep(5)
        logging.info(key + " failover!")
        sys.stdout.flush()
        raise Exception("fail over!")

    for i in range(5):
        logging.info(key + " run num: " + str(i))
        sys.stdout.flush()
        time.sleep(5)


if __name__ == "__main__":
    pass
