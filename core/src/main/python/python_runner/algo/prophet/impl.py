import pandas as pd
from prophet import Prophet
from prophet.serialize import model_to_json


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
            m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
            m.fit(df)
            print("impl train end", flush=True)
            self._collector.collectRow(model_to_json(m))
        except BaseException as ex:
            print("{}".format(ex), flush=True)
        else:
            print("impl finished", flush=True)

    class Java:
        implements = ["com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle"]
