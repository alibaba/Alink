package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatGenericWithClassLoader;
import com.alibaba.alink.common.io.xls.XlsReaderClassLoader;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.params.io.XlsSourceParams;

@IoOpAnnotation(name = "xls_source", ioType = IOType.SourceStream)
@NameCn("Xls和Xlsx表格读入")
@NameEn("Xls and Xlsx File Source")
public class XlsSourceStreamOp extends BaseSourceStreamOp <XlsSourceStreamOp>
	implements XlsSourceParams <XlsSourceStreamOp> {
	public XlsSourceStreamOp() {
		this(new Params());
	}

	private final XlsReaderClassLoader factory;

	public XlsSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(XlsSourceStreamOp.class), params);
		factory = new XlsReaderClassLoader("0.11");
	}

	@Override
	protected Table initializeDataSource() {

		Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> sourceFunction = XlsReaderClassLoader
			.create(factory)
			.createInputFormat(getParams());

		RichInputFormat <Row, InputSplit> inputFormat
			= new RichInputFormatGenericWithClassLoader <>(factory, sourceFunction.f0);

		DataStream data = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getStreamExecutionEnvironment()
			.createInput(inputFormat, new RowTypeInfo(sourceFunction.f1.getFieldTypes()))
			.name("xls-file-source")
			.rebalance();
		;

		return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, sourceFunction.f1);
	}
}
