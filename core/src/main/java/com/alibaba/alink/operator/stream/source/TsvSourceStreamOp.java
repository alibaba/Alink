package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.TsvSourceParams;

/**
 * Stream source that reads reads data with tab-separated values.
 */
@IoOpAnnotation(name = "tsv", ioType = IOType.SourceStream)
@NameCn("TSV文件数据源")
@NameEn("TSV Source")
public final class TsvSourceStreamOp extends BaseSourceStreamOp <TsvSourceStreamOp>
	implements TsvSourceParams <TsvSourceStreamOp> {

	private static final long serialVersionUID = 734742973323350414L;

	public TsvSourceStreamOp() {
		this(new Params());
	}

	public TsvSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(TsvSourceStreamOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		return new CsvSourceStreamOp()
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
