package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.parquet.ParquetClassLoaderFactory;
import com.alibaba.alink.common.io.parquet.ParquetReaderFactory;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatGenericWithClassLoader;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.ParquetSourceParams;

import java.io.IOException;
import java.util.List;

@NameCn("parquet文件读入")
public class ParquetSourceBatchOp extends BaseSourceBatchOp <ParquetSourceBatchOp>
	implements ParquetSourceParams <ParquetSourceBatchOp> {

	public ParquetSourceBatchOp() {
		this(new Params());
	}

	private final ParquetClassLoaderFactory factory;

	public ParquetSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(ParquetSourceBatchOp.class), params);
		factory = new ParquetClassLoaderFactory("0.11.0");
	}

	@Override
	protected Table initializeDataSource() {
		if (getPartitions() == null) {
			Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> sourceFunction = factory
				.createParquetSourceFactory()
				.createParquetSourceFunction(getParams());

			RichInputFormat <Row, InputSplit> inputFormat
				= new RichInputFormatGenericWithClassLoader <>(factory, sourceFunction.f0);

			DataSet data = MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getExecutionEnvironment()
				.createInput(inputFormat, new RowTypeInfo(sourceFunction.f1.getFieldTypes()));

			return DataSetConversionUtil.toTable(getMLEnvironmentId(), data, sourceFunction.f1);
		}
		FilePath filePath = getFilePath();
		BatchOperator <?> selected;
		TableSchema schema;
		try {
			selected = AkUtils.selectPartitionBatchOp(getMLEnvironmentId(), filePath, getPartitions());
		} catch (IOException e) {
			throw new AkIllegalOperatorParameterException("Could not find the path: " + filePath.getPathStr());
		}
		final String[] colNames = selected.getColNames();
		final ParquetClassLoaderFactory localFactory = factory;
		DataSet data = selected.getDataSet()
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
								ParquetReaderFactory reader = localFactory.createParquetReaderFactory();
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
		return DataSetConversionUtil.toTable(getMLEnvironmentId(), data, schema);
	}
}
