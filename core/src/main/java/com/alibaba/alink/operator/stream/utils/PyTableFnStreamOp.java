package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge;
import com.alibaba.alink.common.pyrunner.fn.PyFnFactory;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.udf.BinaryTableFunctionParams;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
public class PyTableFnStreamOp extends StreamOperator <PyTableFnStreamOp>
	implements BinaryTableFunctionParams <PyTableFnStreamOp> {

	public PyTableFnStreamOp() {
		super(null);
	}

	public PyTableFnStreamOp(Params params) {
		super(params);
	}

	@Override
	public PyTableFnStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		JsonObject config = new JsonObject();
		config.addProperty("language", "python");
		config.addProperty("classObject", getClassObject());
		config.addProperty("classObjectType", getClassObjectType());
		config.addProperty("resultType", String.join(",", getResultTypes()));
		config.addProperty("pythonVersion", getPythonVersion());
		final String configJson = config.toString();

		Map <String, String> runConfig = new HashMap <>();
		if (getParams().contains(HasPythonEnv.PYTHON_ENV)) {
			runConfig.put(BasePythonBridge.PY_VIRTUAL_ENV_KEY, getPythonEnv());
		}

		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		StreamTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		TableFunction <Row> f = PyFnFactory.makeTableFn(funcName, configJson, getResultTypes(), runConfig::getOrDefault);
		tEnv.registerFunction(funcName, f);

		UDTFStreamOp udtfStreamOp = new UDTFStreamOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setReservedCols(getReservedCols())
			.setOutputCols(getOutputCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		String joinType = getParams().getStringOrDefault("joinType", "CROSS");
		udtfStreamOp.getParams().set("joinType", joinType);

		this.setOutputTable(udtfStreamOp.linkFrom(in).getOutputTable());
		return this;
	}
}
