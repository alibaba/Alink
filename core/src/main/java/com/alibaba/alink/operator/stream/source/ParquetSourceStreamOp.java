package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.AkUtils2;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.parquet.ParquetClassLoaderFactory;
import com.alibaba.alink.common.io.parquet.ParquetReaderFactory;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatGenericWithClassLoader;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.ParquetSourceParams;

import java.io.IOException;

@NameCn("parquet文件读入")
@NameEn("Parquet Source")
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
		if(getPartitions()==null){
			Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> sourceFunction = factory
				.createParquetSourceFactory()
				.createParquetSourceFunction(getParams());

			RichInputFormat <Row, InputSplit> inputFormat
				= new RichInputFormatGenericWithClassLoader <>(factory, sourceFunction.f0);

			DataStream data = MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getStreamExecutionEnvironment()
				.createInput(inputFormat, new RowTypeInfo(sourceFunction.f1.getFieldTypes()));

			return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, sourceFunction.f1);

		}
		FilePath filePath = getFilePath();
		StreamOperator <?> selected;
		TableSchema schema;
		try {
			selected = AkUtils2.selectPartitionStreamOp(getMLEnvironmentId(), filePath, getPartitions());
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
		final String[] colNames = selected.getColNames();
		final ParquetClassLoaderFactory localFactory = factory;
		DataStream data = selected.getDataStream()
			.rebalance()
			.flatMap(new FlatMapFunction <Row, Row>() {
				@Override
				public void flatMap(Row value, Collector <Row> out) throws Exception {
					Path path = filePath.getPath();
					for (int i = 0; i < value.getArity(); ++i) {
						path = new Path(path, String.format("%s=%s", colNames[i], String.valueOf(value.getField(i))));
					}
					AkUtils.getFromFolderForEach(
						new FilePath(path, filePath.getFileSystem()),
						new FileProcFunction <FilePath, Boolean>() {
							@Override
							public Boolean apply(FilePath filePath) throws IOException {
								ParquetReaderFactory reader  = localFactory.createParquetReaderFactory();
								reader.open(filePath);
								while (!reader.reachedEnd()) {
									out.collect(reader.nextRecord());
								}
								reader.close();
								return true;
							}
						});
				}
			});
		schema = localFactory.createParquetReaderFactory().getTableSchemaFromParquetFile(filePath);
		return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, schema);
	}

}
