package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query string.
 */
@ParamSelectColumnSpec(name = "selectCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("字符串近似最近邻预测")
public class StringApproxNearestNeighborPredictBatchOp
	extends ModelMapBatchOp <StringApproxNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <StringApproxNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = 3715339958211990196L;

	public StringApproxNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public StringApproxNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
