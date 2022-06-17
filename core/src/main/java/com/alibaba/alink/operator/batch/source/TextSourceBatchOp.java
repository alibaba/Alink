package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.TextSourceParams;

/**
 * A data sources that reads from text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SourceBatch)
@NameCn("Text文件读入")
public final class TextSourceBatchOp extends BaseSourceBatchOp <TextSourceBatchOp>
	implements TextSourceParams <TextSourceBatchOp> {

	private static final long serialVersionUID = -5172709076450910276L;

	public TextSourceBatchOp() {
		this(new Params());
	}

	public TextSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TextSourceBatchOp.class), params);
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
			.setPartitions(getPartitions())
			.getOutputTable();
	}
}
