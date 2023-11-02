import os

import pandas as pd
from prophet import Prophet
from prophet.serialize import model_to_json


class suppress_stdout_stderr(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).
    '''

    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = [os.dup(1), os.dup(2)]

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        for fd in self.null_fds + self.save_fds:
            os.close(fd)


class PyProphetCalc:

    def setCollector(self, collector):
        self._collector = collector

    def calc(self, arg):
        print("impl start", flush=True)
        try:
            ds_array = []
            y_array = []
            for row in arg:
                ds_array.append(row[0])
                y_array.append(row[1])
            list_of_tuples = list(zip(ds_array, y_array))
            df = pd.DataFrame(list_of_tuples, columns=['ds', 'y'])
            with suppress_stdout_stderr():
                m = Prophet()
                m.fit(df)
            print("impl train end", flush=True)
            self._collector.collectRow(model_to_json(m))
        except BaseException as ex:
            print("{}".format(ex), flush=True)
        else:
            print("impl finished", flush=True)

    class Java:
        implements = ["com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle"]
