package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.ImputerModelMapper;
import com.alibaba.alink.operator.common.dataproc.IndexToStringModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.IndexToStringPredictParams;

/**
 * Map index to string.
 */
@ParamSelectColumnSpec(name = "selectedCol",
	allowedTypeCollections = TypeCollections.LONG_TYPES)
@NameCn("IndexToString预测")
@NameEn("Index To String Prediction")
public final class IndexToStringPredictStreamOp
	extends ModelMapStreamOp <IndexToStringPredictStreamOp>
	implements IndexToStringPredictParams <IndexToStringPredictStreamOp> {

	private static final long serialVersionUID = -1554788528740494195L;

	public IndexToStringPredictStreamOp() {
		super(IndexToStringModelMapper::new, new Params());
	}

	public IndexToStringPredictStreamOp(Params params) {
		super(IndexToStringModelMapper::new, params);
	}

	public IndexToStringPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public IndexToStringPredictStreamOp(BatchOperator model, Params params) {
		super(model, IndexToStringModelMapper::new, params);
	}
}
