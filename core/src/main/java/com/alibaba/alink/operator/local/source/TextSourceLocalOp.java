package com.alibaba.alink.operator.local.source;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.TextSourceParams;

/**
 * A data sources that reads from text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SourceBatch)
@NameCn("Text文件读入")
public final class TextSourceLocalOp extends BaseSourceLocalOp <TextSourceLocalOp>
	implements TextSourceParams <TextSourceLocalOp> {

	private static final long serialVersionUID = -5172709076450910276L;

	public TextSourceLocalOp() {
		this(new Params());
	}

	public TextSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public MTable initializeDataSource() {
		return new CsvSourceLocalOp()
			.setFilePath(getFilePath())
			.setFieldDelimiter("\n")
			.setQuoteChar(null)
			.setIgnoreFirstLine(getIgnoreFirstLine())
			.setSchemaStr(getTextCol() + " string")
			.setPartitions(getPartitions())
			.getOutputTable();
	}
}
