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
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.BasePyTableFnParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
abstract class BasePyTableFnBatchOp<T extends BasePyTableFnBatchOp <T>> extends BatchOperator <T>
	implements BasePyTableFnParams <T> {

	@SuppressWarnings("unused")
	public BasePyTableFnBatchOp() {
		super(null);
	}

	public BasePyTableFnBatchOp(Params params) {
		super(params);
	}

	protected abstract TableFunction <?> getPyTableFn(String funcName);

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		// WARNING: "tEnv" is used in auto-replacement to blink's code, don't change!
		BatchTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment();
		final String funcName = UDFHelper.generateRandomFuncName();
		TableFunction <?> f = getPyTableFn(funcName);
		tEnv.registerFunction(funcName, f);

		UDTFBatchOp udtfBatchOp = new UDTFBatchOp()
			.setFuncName(funcName)
			.setSelectedCols(getSelectedCols())
			.setOutputCols(getOutputCols())
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId());
		this.setOutputTable(udtfBatchOp.linkFrom(in).getOutputTable());
		//noinspection unchecked
		return (T) this;
	}
}
