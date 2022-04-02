package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToTripleParams;

/**
 * Transform data type from Columns to Triple.
 */
@NameCn("列数据转三元组")
public class ColumnsToTripleBatchOp extends AnyToTripleBatchOp <ColumnsToTripleBatchOp>
	implements ColumnsToTripleParams <ColumnsToTripleBatchOp> {

	private static final long serialVersionUID = 7588256483554816392L;

	public ColumnsToTripleBatchOp() {
		this(new Params());
	}

	public ColumnsToTripleBatchOp(Params params) {
		super(FormatType.COLUMNS, params);
	}
}
