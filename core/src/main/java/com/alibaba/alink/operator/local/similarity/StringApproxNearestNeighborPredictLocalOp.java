package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query string.
 */
@ParamSelectColumnSpec(name = "selectCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("字符串近似最近邻预测")
public class StringApproxNearestNeighborPredictLocalOp
	extends ModelMapLocalOp <StringApproxNearestNeighborPredictLocalOp>
	implements NearestNeighborPredictParams <StringApproxNearestNeighborPredictLocalOp> {

	public StringApproxNearestNeighborPredictLocalOp() {
		this(new Params());
	}

	public StringApproxNearestNeighborPredictLocalOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
