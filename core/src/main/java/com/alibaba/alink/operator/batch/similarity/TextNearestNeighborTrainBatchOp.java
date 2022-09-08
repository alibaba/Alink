package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextNearestNeighborTrainParams;

/**
 * Find the approximate nearest neighbor of query texts.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本最近邻训练")
public class TextNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp <TextNearestNeighborTrainBatchOp>
	implements StringTextNearestNeighborTrainParams<TextNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = -4703892876178263062L;

	public TextNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public TextNearestNeighborTrainBatchOp(Params params) {
		super(params.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.TEXT)
			.set(StringModelDataConverter.TEXT, true));
	}
}
