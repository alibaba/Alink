package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;

/**
 * Transform data type from Columns to Vector.
 */
@NameCn("列数据转向量")
public class ColumnsToVectorStreamOp extends BaseFormatTransStreamOp <ColumnsToVectorStreamOp>
	implements ColumnsToVectorParams <ColumnsToVectorStreamOp> {

	private static final long serialVersionUID = -8365625871912296977L;

	public ColumnsToVectorStreamOp() {
		this(new Params());
	}

	public ColumnsToVectorStreamOp(Params params) {
		super(FormatType.COLUMNS, FormatType.VECTOR, params);
	}
}
