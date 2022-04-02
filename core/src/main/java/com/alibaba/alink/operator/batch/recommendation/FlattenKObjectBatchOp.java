package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.operator.common.recommendation.FlattenKObjectMapper;
import com.alibaba.alink.params.recommendation.FlattenKObjectParams;

/**
 * Transform json format recommendation to table format.
 */
@ParamSelectColumnSpec(name = "selectedCol",
	allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("展开KObject")
public class FlattenKObjectBatchOp extends FlatMapBatchOp <FlattenKObjectBatchOp>
	implements FlattenKObjectParams <FlattenKObjectBatchOp> {

	private static final long serialVersionUID = 790348573681664909L;

	public FlattenKObjectBatchOp() {
		this(null);
	}

	public FlattenKObjectBatchOp(Params params) {
		super(FlattenKObjectMapper::new, params);
	}
}
