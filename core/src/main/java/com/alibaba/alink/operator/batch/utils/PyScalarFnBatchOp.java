package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.java.BatchTableEnvironment;
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
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.udf.BinaryScalarFunctionParams;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
public class PyScalarFnBatchOp extends BatchOperator <PyScalarFnBatchOp>
	implements BinaryScalarFunctionParams <PyScalarFnBatchOp> {

	public PyScalarFnBatchOp() {
		super(null);
	}

	public PyScalarFnBatchOp(Params params) {
		super(params);
	}

	@Override
	public PyScalarFnBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		JsonObject fnSpec = new JsonObject();
		fnSpec.add("classObject", JsonConverter.gson.toJsonTree(getClassObject()));
		fnSpec.addProperty("classObjectType", getClassObjectType());
		fnSpec.addProperty("pythonVersion", getPythonVersion());
		final String fnSpecJson = fnSpec.toString();

		Map <String, String> runConfig = new HashMap<>();
		if (getParams().contains(HasPythonEnv.PYTHON_ENV)) {
			runConfig.put(BasePythonBridge.PY_VIRTUAL_ENV_KEY, getPythonEnv());
		}

		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		BatchTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		ScalarFunction f = PyFnFactory.makeScalarFn(funcName, getResultType(), fnSpecJson, runConfig::getOrDefault);
		tEnv.registerFunction(funcName, f);

		UDFBatchOp udfBatchOp = new UDFBatchOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCol(getOutputCol())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udfBatchOp.linkFrom(in).getOutputTable());
		return this;
	}
}
