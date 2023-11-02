package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.udf.BasePyTableFnParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
abstract class BasePyTableFnStreamOp<T extends BasePyTableFnStreamOp <T>> extends StreamOperator <T>
	implements BasePyTableFnParams <T> {

	@SuppressWarnings("unused")
	public BasePyTableFnStreamOp() {
		this(new Params());
	}

	public BasePyTableFnStreamOp(Params params) {
		super(params);
	}

	protected abstract TableFunction <?> getPyTableFn(String funcName);

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		StreamTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		TableFunction <?> f = getPyTableFn(funcName);
		tEnv.registerFunction(funcName, f);

		UDTFStreamOp udtfStreamOp = new UDTFStreamOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCols(getOutputCols())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udtfStreamOp.linkFrom(in).getOutputTable());
		//noinspection unchecked
		return (T) this;
	}
}
