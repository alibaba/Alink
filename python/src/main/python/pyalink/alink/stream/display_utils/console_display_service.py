from pyalink.alink.stream.display_utils.abstract_display_service import AbstractDisplayService


class ConsoleDisplayService(AbstractDisplayService):
    def __init__(self, op, key, refresh_interval, max_limit):
        print("DataStream " + key + ":")
        colnames = op.getColNames()
        print(colnames)

    def accept_items(self, items):
        for item in items:
            print(item)

    def stop(self):
        pass
