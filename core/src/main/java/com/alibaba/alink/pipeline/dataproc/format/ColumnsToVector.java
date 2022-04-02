package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;

/**
 * Transform data type from Columns to Vector.
 */
@NameCn("列数据转向量")
public class ColumnsToVector extends BaseFormatTrans <ColumnsToVector>
	implements ColumnsToVectorParams <ColumnsToVector> {

	private static final long serialVersionUID = 7213617611629715207L;

	public ColumnsToVector() {
		this(new Params());
	}

	public ColumnsToVector(Params params) {
		super(FormatType.COLUMNS, FormatType.VECTOR, params);
	}
}

