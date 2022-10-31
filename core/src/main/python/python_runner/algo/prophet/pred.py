import json
import os
from array import array

import pandas as pd
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json


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
        print("Entering Python calc", flush=True)
        print(conf)
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

            capVal = conf['cap']
            if capVal is not None:
                capVal = float(capVal)

            floorVal = conf['floor']
            if floorVal is not None:
                floorVal = float(floorVal)

            holidays_prior_scale = float(conf['holidays_prior_scale'])
            n_changepoints = int(conf['n_change_point'])
            changepoint_range = float(conf['change_point_range'])
            changepoint_prior_scale = float(conf['changepoint_prior_scale'])
            interval_width = float(conf['interval_width'])
            seasonality_prior_scale = float(conf['seasonality_prior_scale'])
            seasonality_mode = conf['seasonality_mode']
            mcmc_samples = int(conf['mcmc_samples'])

            yearly_seasonality = conf['yearly_seasonality']
            weekly_seasonality = conf['weekly_seasonality']
            daily_seasonality = conf['daily_seasonality']
            if  yearly_seasonality == 'true':
                yearly_seasonality = True
            if  yearly_seasonality == 'false':
                yearly_seasonality = False
            if  weekly_seasonality == 'true':
                weekly_seasonality = True
            if  weekly_seasonality == 'false':
                weekly_seasonality = False
            if  daily_seasonality == 'true':
                daily_seasonality = True
            if  daily_seasonality == 'false':
                daily_seasonality = False

            include_history = conf['include_history']
            if include_history == 'true':
                include_history = True
            if include_history == 'false':
                include_history = False

            changepoints = conf['changepoints']
            if changepoints is not None:
                changepoints = changepoints.split(',')

            stan_i = conf['init_model']
            if stan_i is not None:
                stan_i = self.stan_init2(json.loads(stan_i))
                dimDelta = len(stan_i['delta'])

            # holidays
            holidays_str = conf['holidays']
            holidays_pd = None
            if holidays_str is not None:
                holidays_str_splits = holidays_str.split(' ')
                for single_holiday in holidays_str_splits:
                    s3 = single_holiday.split(':')
                    holiday_name = s3[0]
                    holiday_vals = s3[1].split(",")
                    holiday_pd = pd.DataFrame({
                      'holiday': holiday_name,
                      'ds': pd.to_datetime(holiday_vals),
                      'lower_window': 0,
                      'upper_window': 1,
                    })
                    if holidays_pd is None:
                        holidays_pd = holiday_pd
                    else:
                        holidays_pd = pd.concat((holidays_pd, holiday_pd))

            #data
            ds_array = []
            y_array = []
            for row in argv1:
                ds_array.append(row[0])
                y_array.append(row[1])
            list_of_tuples = list(zip(ds_array, y_array))
            df = pd.DataFrame(list_of_tuples, columns=['ds', 'y'])
            if capVal is not None:
                df['cap'] = capVal
            if floorVal is not None:
                df['floor'] = floorVal

            dataLen = len(list_of_tuples)

            # init model
            with suppress_stdout_stderr():
                m = Prophet(growth = growth,
                            uncertainty_samples = uncertainty_samples,
                            holidays = holidays_pd,
                            n_changepoints = n_changepoints,
                            changepoint_range = changepoint_range,
                            seasonality_mode = seasonality_mode,
                            seasonality_prior_scale = seasonality_prior_scale,
                            holidays_prior_scale = holidays_prior_scale,
                            changepoint_prior_scale = changepoint_prior_scale,
                            interval_width = interval_width,
                            changepoints =  changepoints,
                            yearly_seasonality = yearly_seasonality,
                            weekly_seasonality = weekly_seasonality,
                            daily_seasonality = daily_seasonality,
                            mcmc_samples = mcmc_samples)
                if stan_i is not None and dataLen == dimDelta + 2:
                    m.fit(df, init=stan_i)
                elif argv2 is None or argv2[0][0] is None:
                    m.fit(df)
                else:
                    init_model_str = argv2[0][0]
                    init_model = model_from_json(init_model_str)

                    # fit and pred
                    m.fit(df, init=stan_init(init_model))

            future = m.make_future_dataframe(periods=predict_num, freq=freq, include_history=False)
            if capVal is not None:
                future['cap'] = capVal
            if floorVal is not None:
                future['floor'] = floorVal
            pout = m.predict(future)

            if include_history:
                future2 = m.make_future_dataframe(periods=predict_num, freq=freq, include_history=True)
                if capVal is not None:
                    future2['cap'] = capVal
                if floorVal is not None:
                    future2['floor'] = floorVal
                pout2 = m.predict(future2)
                self._collector.collectRow(model_to_json(m), pout2.to_json(), json.dumps(pout.yhat.values.tolist()))
            else:
                self._collector.collectRow(model_to_json(m), pout.to_json(), json.dumps(pout.yhat.values.tolist()))
        except BaseException as ex:
            print({}.format(ex), flush=True)
            raise ex
        finally:
            print("Leaving Python calc", flush=True)

    class Java:
        implements = ["com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle"]
