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
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.parquet.ParquetClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatGenericWithClassLoader;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.params.io.ParquetSourceParams;

@NameCn("parquet文件读入")
public class ParquetSourceStreamOp extends BaseSourceStreamOp <ParquetSourceStreamOp>
	implements ParquetSourceParams <ParquetSourceStreamOp> {

	public ParquetSourceStreamOp() {
		this(new Params());
	}

	private final ParquetClassLoaderFactory factory;

	public ParquetSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(ParquetSourceStreamOp.class), params);

		factory = new ParquetClassLoaderFactory("0.11.0");
	}

	@Override
	protected Table initializeDataSource() {

		Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> sourceFunction = ParquetClassLoaderFactory
			.create(factory)
			.createParquetSourceFunction(getParams());

		RichInputFormat <Row, InputSplit> inputFormat
			= new RichInputFormatGenericWithClassLoader <>(factory, sourceFunction.f0);

		DataStream data = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getStreamExecutionEnvironment()
			.createInput(inputFormat, new RowTypeInfo(sourceFunction.f1.getFieldTypes()));

		return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, sourceFunction.f1);

	}

}
