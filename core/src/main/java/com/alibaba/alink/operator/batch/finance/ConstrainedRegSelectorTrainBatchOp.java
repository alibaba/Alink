package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.BaseStepWiseSelectorBatchOp;
import com.alibaba.alink.params.finance.BaseStepwiseSelectorParams;
import com.alibaba.alink.params.finance.ConstrainedRegSelectorTrainParams;

import java.security.InvalidParameterException;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "forceSelectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("带约束的Stepwise回归筛选训练")
@NameEn("Constrained Linear Selector Trainer")
public class ConstrainedRegSelectorTrainBatchOp extends BatchOperator <ConstrainedRegSelectorTrainBatchOp>
	implements ConstrainedRegSelectorTrainParams <ConstrainedRegSelectorTrainBatchOp> {

	private static final long serialVersionUID = 368730481323862021L;

	public ConstrainedRegSelectorTrainBatchOp() {
		super(null);
	}

	public ConstrainedRegSelectorTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public ConstrainedRegSelectorTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (inputs.length != 2) {
			throw new InvalidParameterException("input size must be two.");
		}

		BatchOperator op = new BaseStepWiseSelectorBatchOp(
			this.getParams())
			.setStepWiseType(getMethod().name())
			.setLinearModelType(BaseStepwiseSelectorParams.LinearModelType.LinearReg)
			.setOptimMethod(getOptimMethod().name())
			.linkFrom(inputs);

		setOutputTable(op.getOutputTable());
		setSideOutputTables(new Table[] {op.getSideOutput(1).getOutputTable()});

		return this;
	}
}
