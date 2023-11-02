package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.similarity.BaseNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Find the approximate nearest neighbor of query texts.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本最近邻训练")
@NameEn("Text Nearest Neighbor Prediction Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.similarity.TextNearestNeighbor")
public class TextNearestNeighborTrainLocalOp
	extends BaseNearestNeighborTrainLocalOp<TextNearestNeighborTrainLocalOp>
	implements StringTextNearestNeighborTrainParams<TextNearestNeighborTrainLocalOp> {

	public TextNearestNeighborTrainLocalOp() {
		this(new Params());
	}

	public TextNearestNeighborTrainLocalOp(Params params) {
		super(params.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.TEXT)
			.set(StringModelDataConverter.TEXT, true));
	}
}
