package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToKvParams;

/**
 * Transform data type from Columns to Kv.
 */
@NameCn("列数据转KV")
@NameEn("table to kv")
@ParamSelectColumnSpec(name = "selectedCols")
public class ColumnsToKvBatchOp extends BaseFormatTransBatchOp <ColumnsToKvBatchOp>
	implements ColumnsToKvParams <ColumnsToKvBatchOp> {

	private static final long serialVersionUID = -1870436589306991455L;

	public ColumnsToKvBatchOp() {
		this(new Params());
	}

	public ColumnsToKvBatchOp(Params params) {
		super(FormatType.COLUMNS, FormatType.KV, params);
	}
}
