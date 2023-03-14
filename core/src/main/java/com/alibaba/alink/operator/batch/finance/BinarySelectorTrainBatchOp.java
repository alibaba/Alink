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
import com.alibaba.alink.params.finance.BinarySelectorTrainParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "forceSelectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("Stepwise二分类筛选训练")
@NameEn("Stepwise Binary Selector Train")
public class BinarySelectorTrainBatchOp extends BatchOperator <BinarySelectorTrainBatchOp>
	implements BinarySelectorTrainParams <BinarySelectorTrainBatchOp> {

	private static final long serialVersionUID = -7632837469256501259L;

	public BinarySelectorTrainBatchOp() {
		super(null);
	}

	public BinarySelectorTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public BinarySelectorTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		BatchOperator op = in.link(new BaseStepWiseSelectorBatchOp(
			this.getParams())
			.setStepWiseType(getMethod().name())
			.setLinearModelType(BaseStepwiseSelectorParams.LinearModelType.LR)
			.setOptimMethod(getOptimMethod().name())
		);

		setOutputTable(op.getOutputTable());
		setSideOutputTables(new Table[] {op.getSideOutput(1).getOutputTable()});

		return this;
	}
}
