package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle;
import com.alibaba.alink.common.pyrunner.PyMIMOCalcRunner;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.timeseries.ProphetParams;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_CMD_KEY;
import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;

public class ProphetMapper extends TimeSeriesSingleMapper {

	private transient CloseableThreadLocal <PyMIMOCalcRunner <PyMIMOCalcHandle>> runner;
	private int predictNum;

	public ProphetMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.predictNum = params.get(ProphetParams.PREDICT_NUM);
	}

	@Override
	public void open() {
		super.open();
		this.runner = new CloseableThreadLocal <>(this::createPythonRunner, this::destroyPythonRunner);
	}

	private PyMIMOCalcRunner <PyMIMOCalcHandle> createPythonRunner() {
		Map <String, String> config = new HashMap <>();
		config.put(PY_TURN_ON_LOGGING_KEY, String.valueOf(AlinkGlobalConfiguration.isPrintProcessInfo()));

		PyMIMOCalcRunner <PyMIMOCalcHandle> runner =
			new PyMIMOCalcRunner <>("algo.prophet.PyProphetCalc2", config);
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
		if (historyVals.length <= 2) {
			return Tuple2.of(null, null);
		}

		Map <String, String> conf = new HashMap <String, String>();
		conf.put("periods", String.valueOf(this.predictNum));
		conf.put("freq", getFreq(historyTimes));
		conf.put("uncertainty_samples", String.valueOf(this.params.get(ProphetParams.UNCERTAINTY_SAMPLES)));
		conf.put("init_model", this.params.get(ProphetParams.STAN_INIT));

		List <Row> inputs = new ArrayList <>();
		SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (int i = 0; i < historyTimes.length; i++) {
			inputs.add(Row.of(dataFormat.format(historyTimes[i].getTime()), historyVals[i]));
		}

		Tuple3 <String, String, double[]> tuple3 = warmStartProphet(this.runner, conf, inputs, null);

		return Tuple2.of(tuple3.f2, tuple3.f1);
	}

	//https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
	static String getFreq(Timestamp[] historyTimes) {
		int len = historyTimes.length;
		long diff = historyTimes[len - 1].getTime() - historyTimes[len - 2].getTime();
		if (diff <= 0) {
			throw new RuntimeException("history times must be acs, and not equal.");
		}
		return diff + "L";
	}

	//<model, result>
	static Tuple3 <String, String, double[]> warmStartProphet(
		CloseableThreadLocal <PyMIMOCalcRunner <PyMIMOCalcHandle>> runner,
		Map <String, String> conf,
		List <Row> inputs, String initModel) {

		List <Row> outputs;
		if (initModel != null) {
			outputs = runner.get().calc(conf, inputs, null);
		} else {
			List <Row> modelRows = new ArrayList <>();
			modelRows.add(Row.of(initModel));
			outputs = runner.get().calc(conf, inputs, modelRows);
		}

		String model = (String) outputs.get(0).getField(0);
		String detail = (String) outputs.get(0).getField(1);
		double[] predictVals = JsonConverter.fromJson((String) outputs.get(0).getField(2), double[].class);

		return Tuple3.of(model, detail, predictVals);
	}

}


