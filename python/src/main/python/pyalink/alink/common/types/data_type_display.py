from abc import ABC, abstractmethod


class DataTypeDisplay(ABC):

    @abstractmethod
    def toDisplayData(self, n: int = None):
        """
        Display current object with multi lines.
        :param n: number of lines
        :return: multi-line presentation of data
        """
        ...

    @abstractmethod
    def toDisplaySummary(self) -> str:
        """
        Display current object data info.
        :return: data summary
        """
        ...

    @abstractmethod
    def toShortDisplayData(self) -> str:
        """
        Display current object in a short line.
        :return: short presentation of data
        """
        ...

    def print(self, n: int = None, file=None):
        if n is None:
            print(self.toDisplayData(), file=file)
        else:
            print(self.toDisplayData(n), file=file)
