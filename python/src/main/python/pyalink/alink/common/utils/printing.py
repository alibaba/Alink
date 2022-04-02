import pandas as pd

from .packages import in_ipython


def print_with_title(d, title: str = None, print_all=None):
    """
    Print anything with title prepended
    :param d: anything
    :param title: title
    :param print_all: whether to print all data without ellipsis, currently only effeci
    """
    if print_all:
        with pd.option_context('display.max_rows', None):
            print_with_title(d, title)
        return

    if title is not None:
        print(title)
    if in_ipython() and isinstance(d, pd.DataFrame):
        from IPython import display
        # noinspection PyTypeChecker
        display.display(d)
    else:
        print(d)
