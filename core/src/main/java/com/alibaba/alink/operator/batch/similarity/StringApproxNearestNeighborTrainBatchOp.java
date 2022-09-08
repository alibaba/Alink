package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;

/**
 * Find the approximate nearest neighbor of query string.
 */
@ParamSelectColumnSpec(name = "selectCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("字符串近似最近邻训练")
public class StringApproxNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp <StringApproxNearestNeighborTrainBatchOp>
	implements StringTextApproxNearestNeighborTrainParams <StringApproxNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = -7647838869499679799L;

	public StringApproxNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public StringApproxNearestNeighborTrainBatchOp(Params params) {
		super(params.set(StringModelDataConverter.TEXT, false)
			.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.APPROX_STRING));
	}
}
