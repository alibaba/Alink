package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.similarity.BaseNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Find the approximate nearest neighbor of query string.
 */
@ParamSelectColumnSpec(name = "selectCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("字符串近似最近邻训练")
@NameEn("String Approx Nearest Neighbor Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.similarity.StringApproxNearestNeighbor")
public class StringApproxNearestNeighborTrainLocalOp
	extends BaseNearestNeighborTrainLocalOp<StringApproxNearestNeighborTrainLocalOp>
	implements StringTextApproxNearestNeighborTrainParams<StringApproxNearestNeighborTrainLocalOp> {

	public StringApproxNearestNeighborTrainLocalOp() {
		this(new Params());
	}

	public StringApproxNearestNeighborTrainLocalOp(Params params) {
		super(params.set(StringModelDataConverter.TEXT, false)
			.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.APPROX_STRING));
	}
}
