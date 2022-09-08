package com.alibaba.alink.operator.local.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.TsvSinkBatchParams;

/**
 * Sink data to files in tab-separated values.
 */
@NameCn("TSV文件导出")
public final class TsvSinkLocalOp extends BaseSinkLocalOp <TsvSinkLocalOp>
	implements TsvSinkBatchParams <TsvSinkLocalOp> {

	public TsvSinkLocalOp() {
		this(new Params());
	}

	public TsvSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public TsvSinkLocalOp sinkFrom(LocalOperator <?> in) {
		in.link(
			new CsvSinkLocalOp()
				.setFilePath(getFilePath())
				.setOverwriteSink(getOverwriteSink())
				.setFieldDelimiter("\t")
				.setQuoteChar(null)
				.setNumFiles(getNumFiles())
				.setPartitionCols(getPartitionCols())
		);
		return this;
	}
}
