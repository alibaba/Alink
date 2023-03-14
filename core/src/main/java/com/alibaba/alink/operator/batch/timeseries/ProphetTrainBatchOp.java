package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.pyrunner.PythonMIMOUdaf;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.timeseries.ProphetTrainParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.annotation.PortType.DATA;
import static com.alibaba.alink.common.annotation.PortType.MODEL;
import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;
import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_VIRTUAL_ENV_KEY;

@InputPorts(values = @PortSpec(value = DATA))
@OutputPorts(values = @PortSpec(value = MODEL))
@NameCn("Prophet训练")
@NameEn("Prophet Training")
public class ProphetTrainBatchOp extends BatchOperator <ProphetTrainBatchOp>
	implements ProphetTrainParams <ProphetTrainBatchOp> {

	public ProphetTrainBatchOp() {
		this(null);
	}

	public ProphetTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public ProphetTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		long envId = in.getMLEnvironmentId();

		Map <String, String> config = new HashMap <>();
		config.put(PY_TURN_ON_LOGGING_KEY, String.valueOf(AlinkGlobalConfiguration.isPrintProcessInfo()));
		if (getParams().contains(HasPythonEnv.PYTHON_ENV)) {
			config.put(PY_VIRTUAL_ENV_KEY, getPythonEnv());
		}

		MLEnvironmentFactory.get(envId).getBatchTableEnvironment().registerFunction(
			"prophet", new PythonMIMOUdaf <>("algo.prophet.PyProphetCalc", config)
		);

		String timeCol = getTimeCol();
		String valCol = getValueCol();

		String groupStr = "__alink_id__";

		StringBuilder formatSbd = new StringBuilder();
		formatSbd.append("cast(1 as BIGINT) as ").append(groupStr);
		for (String name : in.getColNames()) {
			formatSbd.append(",");
			if (timeCol.equals(name)) {
				formatSbd.append("date_format_ltz(").append(timeCol).append(",").append("'yyyy-MM-dd HH:mm:ss'")
					.append(
						") as ").append(timeCol);
			} else {
				formatSbd.append(name);
			}
		}

		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println(formatSbd.toString());
		}

		BatchOperator keyedModel = in
			.select(formatSbd.toString())
			.groupBy(groupStr, groupStr + "," + "prophet(" + timeCol + "," + valCol + ") as model");

		DataSet <Row> out = keyedModel.getDataSet().map(new AddColMapFunction(getParams().toJson()));

		this.setOutput(out,
			new String[] {groupStr, "model", "model_meta"},
			new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING});

		return this;
	}

	static class AddColMapFunction implements MapFunction <Row, Row> {
		private String data;

		public AddColMapFunction(String data) {
			this.data = data;
		}

		@Override
		public Row map(Row value) throws Exception {
			return Row.of(value.getField(0), ((Row) ((List) value.getField(1)).get(0)).getField(0),
				data);
		}
	}

}
