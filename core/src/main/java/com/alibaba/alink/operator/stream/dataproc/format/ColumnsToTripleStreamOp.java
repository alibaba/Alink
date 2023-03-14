package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToTripleParams;

/**
 * Transform data type from Columns to Triple.
 */
@NameCn("列数据转三元组")
@NameEn("Columns to triple")
public class ColumnsToTripleStreamOp extends AnyToTripleStreamOp <ColumnsToTripleStreamOp>
	implements ColumnsToTripleParams <ColumnsToTripleStreamOp> {

	private static final long serialVersionUID = 4500487497707510943L;

	public ColumnsToTripleStreamOp() {
		this(new Params());
	}

	public ColumnsToTripleStreamOp(Params params) {
		super(FormatType.COLUMNS, params);
	}
}
