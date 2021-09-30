import sys
import time
import logging
import logging.config


# test failover with some nodes already finished
def map_func(context):
    key = context.identity
    index = context.index
    fail_num = context.get_failed_num()
    logging.info(key + " fail num: " + str(fail_num))
    sys.stdout.flush()
    if context.get_role_name() == "worker" and 0 == index and fail_num < 1:
        time.sleep(8)
        logging.info(key + " failover!")
        sys.stdout.flush()
        raise Exception("fail over!")

    if context.get_role_name() == "ps":
        while True:
            time.sleep(1)
    else:
        for i in range(2):
            logging.info(key + " run num: " + str(i))
            sys.stdout.flush()
            time.sleep(1)
        logging.info(key + " finished")
        sys.stdout.flush()


if __name__ == "__main__":
    pass
