package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.xls.XlsReaderClassLoader;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.BaseSinkStreamOp;
import com.alibaba.alink.params.io.XlsSinkParams;

@IoOpAnnotation(name = "xls_sink", ioType = IOType.SinkStream)
@NameCn("Xlsx表格写出")
@NameEn("Xlsx Sink")
public class XlsSinkStreamOp extends BaseSinkStreamOp <XlsSinkStreamOp> implements XlsSinkParams <XlsSinkStreamOp> {
	public XlsSinkStreamOp() {
		this(new Params());
	}

	private final XlsReaderClassLoader factory;

	public XlsSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(XlsSinkStreamOp.class), params);
		factory = new XlsReaderClassLoader("0.11");
	}

	@Override
	protected XlsSinkStreamOp sinkFrom(StreamOperator <?> in) {
		FileOutputFormat outputFormat = XlsReaderClassLoader
			.create(factory).createOutputFormat(getParams(), in.getSchema());

		if (getOverwriteSink()) {
			outputFormat.setWriteMode(WriteMode.OVERWRITE);
		} else {
			outputFormat.setWriteMode(WriteMode.NO_OVERWRITE);
		}

		in.getDataStream()
			.addSink(new OutputFormatSinkFunction <>(outputFormat))
			.name("xls-file-sink")
			.setParallelism(getNumFiles());

		return this;
	}
}
