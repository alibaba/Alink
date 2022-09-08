package com.alibaba.alink.operator.local.source;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.TsvSourceParams;

/**
 * A data source that reads data with tab-separated values.
 */
@NameCn("TSV文件读入")
public final class TsvSourceLocalOp extends BaseSourceLocalOp <TsvSourceLocalOp>
	implements TsvSourceParams <TsvSourceLocalOp> {

	public TsvSourceLocalOp() {
		this(new Params());
	}

	public TsvSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public MTable initializeDataSource() {
		return new CsvSourceLocalOp()
			.setFilePath(getFilePath())
			.setFieldDelimiter("\t")
			.setQuoteChar(null)
			.setIgnoreFirstLine(getIgnoreFirstLine())
			.setSchemaStr(getSchemaStr())
			.setSkipBlankLine(getSkipBlankLine())
			.setPartitions(getPartitions())
			.getOutputTable();
	}
}
