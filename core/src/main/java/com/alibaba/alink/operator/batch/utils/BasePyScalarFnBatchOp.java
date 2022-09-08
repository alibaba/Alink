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
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.BasePyScalarFnParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
abstract class BasePyScalarFnBatchOp<T extends BasePyScalarFnBatchOp <T>> extends BatchOperator <T>
	implements BasePyScalarFnParams <T> {

	@SuppressWarnings("unused")
	public BasePyScalarFnBatchOp() {
		super(null);
	}

	public BasePyScalarFnBatchOp(Params params) {
		super(params);
	}

	protected abstract ScalarFunction getPyScalarFn(String funcName);

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		BatchTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		ScalarFunction f = getPyScalarFn(funcName);
		tEnv.registerFunction(funcName, f);

		UDFBatchOp udfBatchOp = new UDFBatchOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCol(getOutputCol())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udfBatchOp.linkFrom(in).getOutputTable());
		//noinspection unchecked
		return (T) this;
	}
}
