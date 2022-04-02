package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

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
import com.alibaba.alink.params.udf.BinaryTableFunctionParams;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
public class PyTableFnBatchOp extends BatchOperator <PyTableFnBatchOp>
	implements BinaryTableFunctionParams <PyTableFnBatchOp> {

	public PyTableFnBatchOp() {
		super(null);
	}

	public PyTableFnBatchOp(Params params) {
		super(params);
	}

	@Override
	public PyTableFnBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		JsonObject fnSpec = new JsonObject();
		fnSpec.add("classObject", JsonConverter.gson.toJsonTree(getClassObject()));
		fnSpec.addProperty("classObjectType", getClassObjectType());
		fnSpec.addProperty("pythonVersion", getPythonVersion());
		final String fnSpecJson = fnSpec.toString();

		Map <String, String> runConfig = new HashMap <>();
		if (getParams().contains(HasPythonEnv.PYTHON_ENV)) {
			runConfig.put(BasePythonBridge.PY_VIRTUAL_ENV_KEY, getPythonEnv());
		}

		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		BatchTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		TableFunction<?> f = PyFnFactory.makeTableFn(funcName, fnSpecJson, getResultTypes(), runConfig::getOrDefault);
		tEnv.registerFunction(funcName, f);

		UDTFBatchOp udtfBatchOp = new UDTFBatchOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCols(getOutputCols())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udtfBatchOp.linkFrom(in).getOutputTable());
		return this;
	}
}
