package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.WoeModelMapper;
import com.alibaba.alink.params.finance.WoePredictParams;

/**
 * Hash a vector in the Jaccard distance space to a new vector of given dimensions.
 */
@Internal
@ParamSelectColumnSpec(name = "selectedCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
public final class WoePredictBatchOp extends ModelMapBatchOp <WoePredictBatchOp>
	implements WoePredictParams <WoePredictBatchOp> {
	private static final long serialVersionUID = -9048145108684979562L;

	public WoePredictBatchOp() {
		this(new Params());
	}

	public WoePredictBatchOp(Params params) {
		super(WoeModelMapper::new, params);
	}
}