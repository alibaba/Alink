package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.pyrunner.PyMIMOCalcHandle;
import com.alibaba.alink.common.pyrunner.PyMIMOCalcRunner;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.timeseries.ProphetParams;
import com.alibaba.alink.params.timeseries.ProphetPredictParams;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;
import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_VIRTUAL_ENV_KEY;

public class ProphetModelMapper extends TimeSeriesModelMapper {

	private transient CloseableThreadLocal <PyMIMOCalcRunner <PyMIMOCalcHandle>> runner;

	private List <Row> modelRow;

	private Map <String, String> state;

	private final int predictNum;

	private Params meta;

	private final ResourcePluginFactory factory;

	public ProphetModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		this.predictNum = params.get(ProphetPredictParams.PREDICT_NUM);
		factory = new ResourcePluginFactory();
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelRow = modelRows;
		this.meta = Params.fromJson((String) modelRows.get(0).getField(2));
	}

	@Override
	public void open() {
		super.open();
		this.runner = new CloseableThreadLocal <>(this::createPythonRunner, this::destroyPythonRunner);
		state = new HashMap <String, String>();
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
	protected Tuple2 <double[], String> predictSingleVar(Timestamp[] historyTimes, double[] historyVals,
														 int predictNum) {
		if (historyVals.length <= 2) {
			return Tuple2.of(null, null);
		}

		Map <String, String> conf = new HashMap <String, String>();
		conf.put("periods", String.valueOf(this.predictNum));
		conf.put("freq", ProphetMapper.getFreq(historyTimes));
		conf.put("uncertainty_samples", String.valueOf(this.meta.get(ProphetParams.UNCERTAINTY_SAMPLES)));
		conf.put("init_model", null);

		List <Row> inputs = new ArrayList <>();
		SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (int i = 0; i < historyTimes.length; i++) {
			inputs.add(Row.of(dataFormat.format(historyTimes[i]), historyVals[i]));
		}

		//model
		String key = "1";
		String currentModel = null;

		if (state.containsKey(key)) {
			currentModel = state.get(key);
		} else {
			for (Row row : this.modelRow) {
				if (row.getField(0).toString().equals(key)) {
					currentModel = (String) row.getField(1);
				}
			}
		}

		Tuple3 <String, String, double[]> tuple3 = ProphetMapper.warmStartProphet(
			this.runner, conf, inputs, currentModel);

		try {
			state.put(key, tuple3.f0);
		} catch (Exception ex) {
			throw new AkIllegalStateException(ex.getMessage());
		}

		return Tuple2.of(tuple3.f2, tuple3.f1);
	}

	@Override
	protected Tuple2 <Vector[], String> predictMultiVar(Timestamp[] historyTimes, Vector[] historyVals,
														int predictNum) {
		throw new AkUnsupportedOperationException("ProphetModelMapper not support predictMultiVar().");
	}

}


