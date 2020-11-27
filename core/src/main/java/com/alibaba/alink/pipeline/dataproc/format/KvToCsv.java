package com.alibaba.alink.pipeline.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.KvToCsvParams;

/**
 * Transform data type from Kv to Csv.
 */
public class KvToCsv extends BaseFormatTrans <KvToCsv> implements KvToCsvParams <KvToCsv> {

	private static final long serialVersionUID = 4216392206038817620L;

	public KvToCsv() {
		this(new Params());
	}

	public KvToCsv(Params params) {
		super(FormatType.KV, FormatType.CSV, params);
	}
}

