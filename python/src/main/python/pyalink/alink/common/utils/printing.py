import pandas as pd

from .packages import in_ipython


def to_multi_lined_df(df: pd.DataFrame):
    """
    Convert elements in `df` with "\n" to multi-rows. `df` is assumed to have no index column.

    Adapted from https://stackoverflow.com/questions/62592738/pandas-dataframe-and-multi-line-values-printout-as-string
    """
    if len(df) == 0:
        return df
    broken_dfs = []
    for col_index in range(df.shape[1]):
        column_series = df.iloc[:, col_index]
        column_series = column_series.apply(str)
        # "AttributeError: Can only use .str accessor with string values!" here, if we do not have strings everywhere
        multi_lined_column_series = column_series.str.split("\n", expand=True).stack()
        broken_dfs.append(multi_lined_column_series)
    # If without keys, column names in the concat become 0, 1
    multi_lined_df = pd.concat(broken_dfs, axis=1, keys=df.columns)
    multi_lined_df = multi_lined_df.fillna("")
    # Keep indices intentionally
    return multi_lined_df


def to_html_br_df(df: pd.DataFrame):
    df = df.apply(str)
    df = df.apply(lambda s: s.replace('\n', '<br>'))
    return df


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

    if isinstance(d, pd.DataFrame):
        if in_ipython():
            from IPython import display
            display.display(display.HTML(d.to_html().replace("\\n", "<br>")))
        else:
            print(to_multi_lined_df(d))
    else:
        print(d)
