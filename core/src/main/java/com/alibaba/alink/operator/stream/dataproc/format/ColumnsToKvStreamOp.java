package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToKvParams;

/**
 * Transform data type from Columns to Kv.
 */
@NameCn("列数据转KV")
@NameEn("Columns to key-value pairs")
public class ColumnsToKvStreamOp extends BaseFormatTransStreamOp <ColumnsToKvStreamOp>
	implements ColumnsToKvParams <ColumnsToKvStreamOp> {

	private static final long serialVersionUID = 4750193622558931114L;

	public ColumnsToKvStreamOp() {
		this(new Params());
	}

	public ColumnsToKvStreamOp(Params params) {
		super(FormatType.COLUMNS, FormatType.KV, params);
	}
}
