package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;

/**
 * Transform data type from Vector to Columns.
 */
public class VectorToColumns extends BaseFormatTrans <VectorToColumns>
	implements VectorToColumnsParams <VectorToColumns> {

	private static final long serialVersionUID = 4349293290232615520L;

	public VectorToColumns() {
		this(new Params());
	}

	public VectorToColumns(Params params) {
		super(FormatType.VECTOR, FormatType.COLUMNS, params);
	}
}

