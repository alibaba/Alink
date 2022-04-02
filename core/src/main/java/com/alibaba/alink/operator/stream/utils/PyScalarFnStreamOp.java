package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge;
import com.alibaba.alink.common.pyrunner.fn.PyFnFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.udf.BinaryScalarFunctionParams;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
public class PyScalarFnStreamOp extends StreamOperator <PyScalarFnStreamOp> implements
	BinaryScalarFunctionParams <PyScalarFnStreamOp> {

	public PyScalarFnStreamOp() {
		super(null);
	}

	public PyScalarFnStreamOp(Params params) {
		super(params);
	}

	@Override
	public PyScalarFnStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		JsonObject config = new JsonObject();
		config.add("classObject", JsonConverter.gson.toJsonTree(getClassObject()));
		config.addProperty("classObjectType", getClassObjectType());
		config.addProperty("pythonVersion", getPythonVersion());
		final String configJson = config.toString();

		Map <String, String> runConfig = new HashMap <>();
		if (getParams().contains(HasPythonEnv.PYTHON_ENV)) {
			runConfig.put(BasePythonBridge.PY_VIRTUAL_ENV_KEY, getPythonEnv());
		}

		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		StreamTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		ScalarFunction f = PyFnFactory.makeScalarFn(funcName, getResultType(), configJson, runConfig::getOrDefault);
		tEnv.registerFunction(funcName, f);

		UDFStreamOp udfStreamOp = new UDFStreamOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCol(getOutputCol())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udfStreamOp.linkFrom(in).getOutputTable());
		return this;
	}
}
