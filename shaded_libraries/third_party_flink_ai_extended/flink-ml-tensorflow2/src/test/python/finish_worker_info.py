import sys
import time


def map_func(context):
    key = context.identity
    index = context.index
    for i in range(11 - index):
        print(key + " finish worker:" + str(context.getFinishWorkerNode()))
        print(key + " finish worker:" + str(len(context.getFinishWorkerNode())))
        print(len(set(context.getFinishWorkerNode())))
        if 0 == index and 5 == i:
            context.stopJob()

        time.sleep(1)


if __name__ == "__main__":
    pass
