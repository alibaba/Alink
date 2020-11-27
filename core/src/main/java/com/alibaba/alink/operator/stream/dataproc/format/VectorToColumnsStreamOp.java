package com.alibaba.alink.operator.stream.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Transform data type from Vector to Columns.
 */
public class VectorToColumnsStreamOp extends BaseFormatTransStreamOp <VectorToColumnsStreamOp>
	implements VectorToColumnsParams <VectorToColumnsStreamOp> {

	private static final long serialVersionUID = -8146831372562929851L;

	public VectorToColumnsStreamOp() {
		this(new Params());
	}

	public VectorToColumnsStreamOp(Params params) {
		super(FormatType.VECTOR, FormatType.COLUMNS, params);
	}
}
