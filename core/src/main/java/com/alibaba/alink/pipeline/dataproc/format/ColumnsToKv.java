package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToKvParams;

/**
 * Transform data type from Columns to Kv.
 */
public class ColumnsToKv extends BaseFormatTrans <ColumnsToKv> implements ColumnsToKvParams <ColumnsToKv> {

	private static final long serialVersionUID = 7120784602225738871L;

	public ColumnsToKv() {
		this(new Params());
	}

	public ColumnsToKv(Params params) {
		super(FormatType.COLUMNS, FormatType.KV, params);
	}
}

