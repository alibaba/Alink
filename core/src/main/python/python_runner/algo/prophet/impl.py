import pandas as pd
from prophet import Prophet
from prophet.serialize import model_to_json


class PyProphetCalc:

    def setCollector(self, collector):
        self._collector = collector

    def calc(self, arg):
        ds_array = []
        y_array = []
        for row in arg:
            ds_array.append(row[0])
            y_array.append(row[1])
        list_of_tuples = list(zip(ds_array, y_array))
        df = pd.DataFrame(list_of_tuples, columns=['ds', 'y'])
        m = Prophet()
        m.fit(df)
        self._collector.collectRow(model_to_json(m))

    class Java:
        implements = ["com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle"]
