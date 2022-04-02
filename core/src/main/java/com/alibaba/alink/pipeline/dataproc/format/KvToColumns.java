package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToColumnsParams;

/**
 * Transform data type from Kv to Columns.
 */
@NameCn("KV转列数据")
public class KvToColumns extends BaseFormatTrans <KvToColumns> implements KvToColumnsParams <KvToColumns> {

	private static final long serialVersionUID = -3775866453771943857L;

	public KvToColumns() {
		this(new Params());
	}

	public KvToColumns(Params params) {
		super(FormatType.KV, FormatType.COLUMNS, params);
	}
}

