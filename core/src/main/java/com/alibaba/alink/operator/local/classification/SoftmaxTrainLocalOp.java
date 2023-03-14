package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.lazy.WithTrainInfoLocalOp;
import com.alibaba.alink.operator.common.linear.LinearModelTrainInfo;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.SoftmaxModelInfo;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.classification.SoftmaxTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.List;

/**
 * Softmax is a classifier for multi-class problem.
 */
@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.MODEL, isOptional = true)
})
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_INFO)
})

@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@FeatureColsVectorColMutexRule

@NameCn("Softmax训练")
@NameEn("Softmax Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.Softmax")
public final class SoftmaxTrainLocalOp extends BaseLinearModelTrainLocalOp <SoftmaxTrainLocalOp>
	implements SoftmaxTrainParams <SoftmaxTrainLocalOp>,
	WithTrainInfoLocalOp <LinearModelTrainInfo, SoftmaxTrainLocalOp>,
	WithModelInfoLocalOp <SoftmaxModelInfo, SoftmaxTrainLocalOp, SoftmaxModelInfoLocalOp> {

	public SoftmaxTrainLocalOp() {
		this(new Params());
	}

	public SoftmaxTrainLocalOp(Params params) {
		super(params, LinearModelType.Softmax, "softmax");
	}

	@Override
	public SoftmaxModelInfoLocalOp getModelInfoLocalOp() {
		return new SoftmaxModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

	@Override
	public LinearModelTrainInfo createTrainInfo(List <Row> rows) {
		return new LinearModelTrainInfo(rows);
	}

	@Override
	public LocalOperator <?> getSideOutputTrainInfo() {
		return this.getSideOutput(0);
	}
}


