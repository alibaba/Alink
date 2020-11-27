package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Transform data type from Vector to Triple.
 */
public class VectorToTripleBatchOp extends AnyToTripleBatchOp <VectorToTripleBatchOp>
	implements VectorToTripleParams <VectorToTripleBatchOp> {

	private static final long serialVersionUID = 2762730453738335970L;

	public VectorToTripleBatchOp() {
		this(new Params());
	}

	public VectorToTripleBatchOp(Params params) {
		super(FormatType.VECTOR, params);
	}

}
