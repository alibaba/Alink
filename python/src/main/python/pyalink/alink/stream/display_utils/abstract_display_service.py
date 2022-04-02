from abc import ABC, abstractmethod


class AbstractDisplayService(ABC):

    @abstractmethod
    def accept_items(self, items):
        raise Exception("Not implemented.")

    @abstractmethod
    def stop(self):
        raise Exception("Not implemented.")
