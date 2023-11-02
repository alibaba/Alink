package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.plugin.wrapper.RichOutputFormatWithClassLoader;
import com.alibaba.alink.common.io.xls.XlsReaderClassLoader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.XlsSinkParams;

@IoOpAnnotation(name = "xls_sink", ioType = IOType.SinkBatch)
@NameCn("Xlsx表格写出")
@NameEn("Xls Sink")
public class XlsSinkBatchOp extends BaseSinkBatchOp <XlsSinkBatchOp> implements XlsSinkParams <XlsSinkBatchOp> {
	public XlsSinkBatchOp() {
		this(new Params());
	}

	private final XlsReaderClassLoader factory;

	public XlsSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(XlsSinkBatchOp.class), params);
		factory = new XlsReaderClassLoader("0.11");
	}

	@Override
	protected XlsSinkBatchOp sinkFrom(BatchOperator <?> in) {
		FileOutputFormat xlsxOutputFormat = XlsReaderClassLoader
			.create(factory).createOutputFormat(getParams(), in.getSchema());

		if (getOverwriteSink()) {
			xlsxOutputFormat.setWriteMode(WriteMode.OVERWRITE);
		} else {
			xlsxOutputFormat.setWriteMode(WriteMode.NO_OVERWRITE);
		}

		RichOutputFormatWithClassLoader outputFormat =
			new RichOutputFormatWithClassLoader(factory, xlsxOutputFormat);

		in.getDataSet().output(outputFormat)
			.name("xls-file-sink")
			.setParallelism(getNumFiles());

		return this;
	}
}
