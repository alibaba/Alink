package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.TsvSourceParams;

/**
 * A data source that reads data with tab-separated values.
 */
@IoOpAnnotation(name = "tsv", ioType = IOType.SourceBatch)
@NameCn("TSV文件读入")
@NameEn("Tsv Source")
public final class TsvSourceBatchOp extends BaseSourceBatchOp <TsvSourceBatchOp>
	implements TsvSourceParams <TsvSourceBatchOp> {

	private static final long serialVersionUID = -6634964130670259227L;

	public TsvSourceBatchOp() {
		this(new Params());
	}

	public TsvSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TsvSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		return new CsvSourceBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
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
