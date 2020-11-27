package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Transform data type from Vector to Triple.
 */
public class VectorToTripleStreamOp extends AnyToTripleStreamOp <VectorToTripleStreamOp>
	implements VectorToTripleParams <VectorToTripleStreamOp> {

	private static final long serialVersionUID = -6373174913353961089L;

	public VectorToTripleStreamOp() {
		this(new Params());
	}

	public VectorToTripleStreamOp(Params params) {
		super(FormatType.VECTOR, params);
	}
}
