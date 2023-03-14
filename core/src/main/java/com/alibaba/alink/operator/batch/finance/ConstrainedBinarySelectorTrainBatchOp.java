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
import com.alibaba.alink.params.finance.ConstrainedBinarySelectorTrainParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "forceSelectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("带约束的Stepwise二分类筛选训练")
@NameEn("Constrained Binary Selector Trainer")
public class ConstrainedBinarySelectorTrainBatchOp extends BatchOperator <ConstrainedBinarySelectorTrainBatchOp>
	implements ConstrainedBinarySelectorTrainParams <ConstrainedBinarySelectorTrainBatchOp> {

	private static final long serialVersionUID = 8418940566466615567L;

	public ConstrainedBinarySelectorTrainBatchOp() {
		super(null);
	}

	public ConstrainedBinarySelectorTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public ConstrainedBinarySelectorTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (inputs.length != 2) {
			throw new RuntimeException("input must be two.");
		}

		BatchOperator op = new BaseStepWiseSelectorBatchOp(
			this.getParams())
			.setStepWiseType(getMethod().name())
			.setLinearModelType(BaseStepwiseSelectorParams.LinearModelType.LR)
			.setOptimMethod(getOptimMethod().name())
			.linkFrom(inputs);

		setOutputTable(op.getOutputTable());
		setSideOutputTables(new Table[] {op.getSideOutput(1).getOutputTable()});

		return this;
	}
}
