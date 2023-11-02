package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle;
import com.alibaba.alink.common.pyrunner.PyMIMOCalcRunner;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.timeseries.ProphetParams;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;
import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_VIRTUAL_ENV_KEY;

public class ProphetMapper extends TimeSeriesSingleMapper {

	private static final Logger LOG = LoggerFactory.getLogger(ProphetMapper.class);

	private transient CloseableThreadLocal <PyMIMOCalcRunner <PyMIMOCalcHandle>> runner;
	private final int predictNum;

	private final ResourcePluginFactory factory;

	public ProphetMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.predictNum = params.get(ProphetParams.PREDICT_NUM);
		factory = new ResourcePluginFactory();
	}

	@Override
	public void open() {
		super.open();
		this.runner = new CloseableThreadLocal <>(this::createPythonRunner, this::destroyPythonRunner);
	}

	private PyMIMOCalcRunner <PyMIMOCalcHandle> createPythonRunner() {
		Map <String, String> config = new HashMap <>();
		config.put(PY_TURN_ON_LOGGING_KEY, String.valueOf(AlinkGlobalConfiguration.isPrintProcessInfo()));
		if (params.contains(HasPythonEnv.PYTHON_ENV)) {
			config.put(PY_VIRTUAL_ENV_KEY, params.get(HasPythonEnv.PYTHON_ENV));
		}

		PyMIMOCalcRunner <PyMIMOCalcHandle> runner =
			new PyMIMOCalcRunner <>("algo.prophet.PyProphetCalc2", config::getOrDefault, factory);
		runner.open();
		return runner;
	}

	private void destroyPythonRunner(PyMIMOCalcRunner <PyMIMOCalcHandle> runner) {
		runner.close();
	}

	@Override
	public void close() {
		this.runner.close();
	}

	@Override
	protected Tuple2 <double[], String> predictSingleVar(Timestamp[] historyTimes,
														 double[] historyVals,
														 int predictNum) {
		LOG.info("Entering predictSingleVar");
		if (historyVals.length <= 2) {
			LOG.info("historyVals.length <= 2");
			return Tuple2.of(null, null);
		}

		Map <String, String> conf = new HashMap <String, String>();
		conf.put("periods", String.valueOf(this.predictNum));
		conf.put("freq", getFreq(historyTimes));
		conf.put("uncertainty_samples", String.valueOf(this.params.get(ProphetParams.UNCERTAINTY_SAMPLES)));
		conf.put("init_model", this.params.get(ProphetParams.STAN_INIT));

		if (params.contains(ProphetParams.HOLIDAYS)) {
			conf.put("holidays", params.get(ProphetParams.HOLIDAYS));
		}

		if (params.contains(ProphetParams.CAP)) {
			conf.put("cap", String.valueOf(params.get(ProphetParams.CAP)));
		}

		if (params.contains(ProphetParams.FLOOR)) {
			conf.put("floor", String.valueOf(params.get(ProphetParams.FLOOR)));
		}

		if(params.contains(ProphetParams.CHANGE_POINTS)) {
			conf.put("changepoints", params.get(ProphetParams.CHANGE_POINTS));
		}

		conf.put("growth", String.valueOf(params.get(ProphetParams.GROWTH)).toLowerCase());
		conf.put("holidays_prior_scale", String.valueOf(params.get(ProphetParams.HOLIDAYS_PRIOR_SCALE)));
		conf.put("n_change_point", String.valueOf(params.get(ProphetParams.N_CHANGE_POINT)));
		conf.put("change_point_range", String.valueOf(params.get(ProphetParams.CHANGE_POINT_RANGE)));
		conf.put("changepoint_prior_scale", String.valueOf(params.get(ProphetParams.CHANGE_POINT_PRIOR_SCALE)));
		conf.put("interval_width", String.valueOf(params.get(ProphetParams.INTERVAL_WIDTH)));
		conf.put("seasonality_mode", String.valueOf(params.get(ProphetParams.SEASONALITY_MODE)).toLowerCase());
		conf.put("seasonality_prior_scale", String.valueOf(params.get(ProphetParams.SEASONALITY_PRIOR_SCALE)));
		conf.put("mcmc_samples", String.valueOf(params.get(ProphetParams.MCMC_SAMPLES)));
		conf.put("yearly_seasonality", params.get(ProphetParams.YEARLY_SEASONALITY));
		conf.put("weekly_seasonality", params.get(ProphetParams.WEEKLY_SEASONALITY));
		conf.put("daily_seasonality", params.get(ProphetParams.DAILY_SEASONALITY));
		conf.put("include_history", String.valueOf(params.get(ProphetParams.INCLUDE_HISTORY)).toLowerCase());

		List <Row> inputs = new ArrayList <>();
		SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (int i = 0; i < historyTimes.length; i++) {
			inputs.add(Row.of(dataFormat.format(historyTimes[i].getTime()), historyVals[i]));
		}

		Tuple3 <String, String, double[]> tuple3 = warmStartProphet(this.runner, conf, inputs, null);
		LOG.info("Leaving predictSingleVar");
		return Tuple2.of(tuple3.f2, tuple3.f1);
	}

	//https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
	static String getFreq(Timestamp[] historyTimes) {
		int len = historyTimes.length;
		long diff = historyTimes[len - 1].getTime() - historyTimes[len - 2].getTime();
		if (diff <= 0) {
			throw new AkIllegalDataException("history times must be acs, and not equal.");
		}
		return diff + "L";
	}

	//<model, result>
	static Tuple3 <String, String, double[]> warmStartProphet(
		CloseableThreadLocal <PyMIMOCalcRunner <PyMIMOCalcHandle>> runner,
		Map <String, String> conf,
		List <Row> inputs, String initModel) {
		LOG.info("Entering warmStartProphet");

		List <Row> outputs;
		if (initModel == null) {
			LOG.info("initModel == null");
			outputs = runner.get().calc(conf, inputs, null);
			LOG.info("after call calc");
		} else {
			LOG.info("initModel != null");
			List <Row> modelRows = new ArrayList <>();
			modelRows.add(Row.of(initModel));
			outputs = runner.get().calc(conf, inputs, modelRows);
			LOG.info("after call calc");
		}

		String model = (String) outputs.get(0).getField(0);
		String detail = (String) outputs.get(0).getField(1);

		JSONObject a = (JSONObject) JSON.parse(detail);
		String[] detailColNames = a.keySet().toArray(new String[0]);
		int detailColNum = detailColNames.length;
		TypeInformation <?>[] detailColTypes = new TypeInformation <?>[detailColNum];
		for (int i = 0; i < detailColNum; i++) {
			detailColTypes[i] = AlinkTypes.DOUBLE;
		}

		Row[] rows = null;

		int detailRowNum = -1;
		for (int i = 0; i < detailColNum; i++) {
			JSONObject b = (JSONObject) a.get(detailColNames[i]);
			if (detailRowNum < 0) {
				detailRowNum = b.size();
				rows = new Row[detailRowNum];
				for (int j = 0; j < detailRowNum; j++) {
					rows[j] = new Row(detailColNum);
				}
			}
			for (String ak : b.keySet()) {
				int rowIdx = Integer.parseInt(ak);
				if (b.get(ak) instanceof BigDecimal) {
					rows[rowIdx].setField(i, ((BigDecimal) b.get(ak)).doubleValue());
				} else {
					long val = ((Number) b.get(ak)).longValue();
					if (detailColNames[i].equals("ds")) {
						detailColTypes[i] = AlinkTypes.SQL_TIMESTAMP;
						val -= 28800000;
						rows[rowIdx].setField(i, new Timestamp(val));
					} else {
						detailColTypes[i] = Types.LONG;
						rows[rowIdx].setField(i, val);
					}
				}
			}
		}

		MTable detailMTable = new MTable(rows, detailColNames, detailColTypes);

		double[] predictVals = JsonConverter.fromJson((String) outputs.get(0).getField(2), double[].class);
		LOG.info("Leaving warmStartProphet");
		return Tuple3.of(model, JsonConverter.toJson(detailMTable), predictVals);
	}

}


