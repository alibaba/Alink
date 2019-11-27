package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.TextSourceParams;


/**
 * A data sources that reads from text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SourceBatch)
public final class TextSourceBatchOp extends BaseSourceBatchOp <TextSourceBatchOp>
	implements TextSourceParams <TextSourceBatchOp> {

	public TextSourceBatchOp() {
		this(new Params());
	}

	public TextSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TextSourceBatchOp.class), params);
	}

	public TextSourceBatchOp(String filePath) {
		this(new Params()
			.set(FILE_PATH, filePath)
		);
	}

	@Override
	public Table initializeDataSource() {
		return new CsvSourceBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setFilePath(getFilePath())
			.setFieldDelimiter("\n")
			.setQuoteChar(null)
			.setIgnoreFirstLine(getIgnoreFirstLine())
			.setSchemaStr(getTextCol() + " string")
			.getOutputTable();
	}
}
