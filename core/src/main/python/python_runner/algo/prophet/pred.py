import pandas as pd
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
import json
from array import array

class PyProphetCalc2:

    def setCollector(self, collector):
        self._collector = collector


    def stan_init(self, m):
        res = {}
        for pname in ['k', 'm', 'sigma_obs']:
            res[pname] = m.params[pname][0][0]
        for pname in ['delta', 'beta']:
            res[pname] = m.params[pname]
        return res


    def stan_init2(self, m):
        res2 = {}
        for pname in ['k', 'm', 'sigma_obs']:
            res2[pname] = m[pname]
        for pname in ['delta', 'beta']:
            res2[pname] = array('d', m[pname])
        return res2

    #argv1: data, argv2: init model
    def calc(self, conf, argv1, argv2):
        try:
            growth = conf['growth']
            if growth is None:
                growth = 'linear'

            predict_num = conf['periods']
            if predict_num is None:
                predict_num = 2
            else:
                predict_num = int(predict_num)

            freq = conf['freq']
            if freq is None:
                freq = 'D';

            uncertainty_samples = conf['uncertainty_samples']
            if uncertainty_samples is None:
                uncertainty_samples = 1000
            else:
                uncertainty_samples = int(uncertainty_samples)


            stan_i = conf['init_model']
            if stan_i is not None:
                stan_i = self.stan_init2(json.loads(stan_i))
                dimDelta = len(stan_i['delta'])

            #data
            ds_array = []
            y_array = []
            for row in argv1:
                ds_array.append(row[0])
                y_array.append(row[1])
            list_of_tuples = list(zip(ds_array, y_array))
            df = pd.DataFrame(list_of_tuples, columns=['ds', 'y'])

            dataLen = len(list_of_tuples)

            #init model
            m = Prophet(growth=growth, uncertainty_samples=uncertainty_samples)
            if stan_i is not None and dataLen == dimDelta + 2:
                m.fit(df, init=stan_i)
            elif argv2 is None or argv2[0][0] is None:
                m.fit(df)
            else :
                init_model_str = argv2[0][0]
                init_model = model_from_json(init_model_str)

                #fit and pred
                m.fit(df, init=stan_init(init_model))

            future = m.make_future_dataframe(periods=predict_num, freq=freq, include_history=False)
            pout = m.predict(future)

            self._collector.collectRow(model_to_json(m), pout.to_json(), json.dumps(pout.yhat.values.tolist()))
        except BaseException as ex:
            print({}.format(ex), flush=True)
            raise ex

    class Java:
        implements = ["com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle"]
