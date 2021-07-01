from py4j.java_gateway import JavaGateway, CallbackServerParameters
import pandas as pd
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
import json
from prophet.serialize import model_to_json, model_from_json
import logging
logging.basicConfig(level=logging.INFO,#控制台打印的日志级别
                    filename='/tmp/python.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )

class PythonListener(object):
    def __init__(self, gateway):
        self.gateway = gateway

    def notify(self, obj):
        print("Notified by Java")
        print(obj)
        ds_y_array = obj.split(";")
        ds_array = []
        y_array = []
        for ds_y in ds_y_array:
            ds_array.append(ds_y.split(",")[0])
            y_array.append(ds_y.split(",")[1])

        list_of_tuples = list(zip(ds_array, y_array))
        df = pd.DataFrame(list_of_tuples, columns=['ds', 'y'])

        df.head()
        m = Prophet()
        m.fit(df)
        return model_to_json(m)

    class Java:
        implements = ["com.alibaba.alink.operator.common.prophet.ExampleListener"]

if __name__ == "__main__":
    gateway = JavaGateway(callback_server_parameters=CallbackServerParameters())
    logging.error('******************************************1')
    logging.error('******************************************2')
    listener = PythonListener(gateway)
    logging.error('******************************************3')
    gateway.entry_point.registerListener(listener)
    logging.error('******************************************4')
    gateway.entry_point.notifyAllListeners()
    logging.error('******************************************5')
    gateway.shutdown()
    logging.error('******************************************6')