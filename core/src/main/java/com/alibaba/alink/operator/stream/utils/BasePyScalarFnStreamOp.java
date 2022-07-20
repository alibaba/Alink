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
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.udf.BinaryScalarFunctionParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
abstract class BasePyScalarFnStreamOp<T extends BasePyScalarFnStreamOp <T>> extends StreamOperator <T> implements
	BinaryScalarFunctionParams <T> {

	public BasePyScalarFnStreamOp() {
		this(new Params());
	}

	public BasePyScalarFnStreamOp(Params params) {
		super(params);
	}

	protected abstract ScalarFunction getPyScalarFn(String funcName);

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		StreamTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		ScalarFunction f = getPyScalarFn(funcName);
		tEnv.registerFunction(funcName, f);

		UDFStreamOp udfStreamOp = new UDFStreamOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCol(getOutputCol())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udfStreamOp.linkFrom(in).getOutputTable());
		//noinspection unchecked
		return (T) this;
	}
}
