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
import com.alibaba.alink.params.finance.RegressionSelectorParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "forceSelectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("Stepwise回归筛选预测")
@NameEn("Regression Selector Trainer")
public class RegressionSelectorTrainBatchOp extends BatchOperator <RegressionSelectorTrainBatchOp>
	implements RegressionSelectorParams <RegressionSelectorTrainBatchOp> {

	private static final long serialVersionUID = 4105397009104570405L;

	public RegressionSelectorTrainBatchOp() {
		super(null);
	}

	public RegressionSelectorTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public RegressionSelectorTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		BatchOperator op = in.link(new BaseStepWiseSelectorBatchOp(
			this.getParams())
			.setStepWiseType(getMethod().name())
			.setLinearModelType(BaseStepwiseSelectorParams.LinearModelType.LinearReg)
			.setOptimMethod(getOptimMethod().name())
		);

		setOutputTable(op.getOutputTable());
		setSideOutputTables(new Table[] {op.getSideOutput(1).getOutputTable()});

		return this;
	}
}
