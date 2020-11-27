package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.format.AnyToTripleFlatMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ToTripleParams;

/**
 * The base class of transform other types to triple.
 */
class AnyToTripleBatchOp<T extends AnyToTripleBatchOp <T>> extends FlatMapBatchOp <T>
	implements ToTripleParams <T> {

	private static final long serialVersionUID = 5469186328796734492L;

	public AnyToTripleBatchOp() {
		this(null);
	}

	public AnyToTripleBatchOp(FormatType formatType, Params params) {
		this(
			(null == params ? new Params() : params)
				.set(FormatTransParams.FROM_FORMAT, formatType)
		);
	}

	public AnyToTripleBatchOp(Params params) {
		super(AnyToTripleFlatMapper::new, params);
	}
}
