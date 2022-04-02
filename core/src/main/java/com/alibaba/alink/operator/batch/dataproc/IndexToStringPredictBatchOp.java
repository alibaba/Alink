package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.IndexToStringModelMapper;
import com.alibaba.alink.params.dataproc.IndexToStringPredictParams;

/**
 * Map index to string.
 */
@ParamSelectColumnSpec(name = "selectedCol",
	allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("IndexToString预测")
public final class IndexToStringPredictBatchOp
	extends ModelMapBatchOp <IndexToStringPredictBatchOp>
	implements IndexToStringPredictParams <IndexToStringPredictBatchOp> {

	private static final long serialVersionUID = 6853794328231422675L;

	public IndexToStringPredictBatchOp() {
		this(new Params());
	}

	public IndexToStringPredictBatchOp(Params params) {
		super(IndexToStringModelMapper::new, params);
	}
}
