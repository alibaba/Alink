package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegWithSummaryResult.LinearRegType;
import com.alibaba.alink.params.regression.LinearRegStepwiseTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL)
})
@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("线性回归Stepwise训练")
@NameEn("Stepwise Linear Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.LinearRegStepwise")
public final class LinearRegStepwiseTrainBatchOp extends BatchOperator <LinearRegStepwiseTrainBatchOp>
	implements LinearRegStepwiseTrainParams <LinearRegStepwiseTrainBatchOp> {

	private static final long serialVersionUID = -1316297704040780024L;

	public LinearRegStepwiseTrainBatchOp() {
		super();
	}

	public LinearRegStepwiseTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public LinearRegStepwiseTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		this.setOutputTable(new LinearRegWithSummaryResult(this.getParams(), LinearRegType.stepwise).linkFrom(in)
			.getOutputTable());
		return this;
	}

}
