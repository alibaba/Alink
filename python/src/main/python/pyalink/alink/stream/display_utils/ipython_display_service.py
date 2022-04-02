from datetime import datetime

import pandas as pd
from IPython import display

from .abstract_display_service import AbstractDisplayService
from ...common.types.conversion.type_converters import schema_type_to_py_type


def update_df_display_handle(display_handle, title_display_handle, df, key, timestamp, counter):
    timestamp_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    title_str = "DataStream {} : ( Updated on {}, #items received: {} )".format(key, timestamp_str, counter)
    title_display_handle.update(title_str)
    display_handle.update(df)


class IPythonDisplayService(AbstractDisplayService):
    minimum_refresh_interval = 0.1

    def update_display(self):
        update_df_display_handle(self.display_handle, self.title_display_handle,
                                 self.df, self.key, self.last_refresh_timestamp, self.counter)

    def __init__(self, op, key, refresh_interval, max_limit):
        self.key = key
        self.max_limit = max_limit

        if refresh_interval > 0:
            self.refresh_interval = refresh_interval
            self.rolling_update = False
        else:
            self.refresh_interval = IPythonDisplayService.minimum_refresh_interval
            self.rolling_update = True

        self.colnames = op.getColNames()
        coltypes = op.getColTypes()
        py_coltypes = map(schema_type_to_py_type, coltypes)
        self.df = pd.DataFrame(columns=self.colnames).astype(dict(zip(self.colnames, py_coltypes)))

        self.title_display_handle = display.display("DataStream " + self.key + ": (waiting data to streaming in...)",
                                                    display_id=True)
        # noinspection PyTypeChecker
        self.display_handle = display.display(self.df, display_id=True)

        self.counter = 0
        self.last_refresh_timestamp = datetime.timestamp(datetime.now())

    def accept_items(self, items):
        current_timestamp = datetime.timestamp(datetime.now())

        for item in items:
            if len(self.df) >= self.max_limit:
                if self.rolling_update:
                    self.df = self.df.append(pd.Series(item, index=self.colnames), ignore_index=True)
                    self.df = self.df.iloc[1:]
            else:
                self.df = self.df.append(pd.Series(item, index=self.colnames), ignore_index=True)

        self.counter += len(items)

        if current_timestamp - self.last_refresh_timestamp >= self.refresh_interval:
            self.last_refresh_timestamp = current_timestamp
            self.update_display()
            if not self.rolling_update:
                self.df = self.df[0:0]

    def stop(self):
        if len(self.df) > 0:
            self.update_display()
