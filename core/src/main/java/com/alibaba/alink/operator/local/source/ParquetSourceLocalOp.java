package com.alibaba.alink.operator.local.source;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.parquet.ParquetClassLoaderFactory;
import com.alibaba.alink.common.io.parquet.ParquetReaderFactory;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatGenericWithClassLoader;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.ParquetSourceParams;

import java.io.IOException;
import java.util.List;

@NameCn("parquet文件读入")
public class ParquetSourceLocalOp extends BaseSourceLocalOp <ParquetSourceLocalOp>
	implements ParquetSourceParams <ParquetSourceLocalOp> {

	public ParquetSourceLocalOp() {
		this(new Params());
	}

	private final ParquetClassLoaderFactory factory;

	public ParquetSourceLocalOp(Params params) {
		super(params);
		factory = new ParquetClassLoaderFactory("0.11.0");
	}

	@Override
	protected MTable initializeDataSource() {
		if (getPartitions() == null) {
			Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> sourceFunction = factory
				.createParquetSourceFactory()
				.createParquetSourceFunction(getParams());

			RichInputFormat <Row, InputSplit> inputFormat
				= new RichInputFormatGenericWithClassLoader <>(factory, sourceFunction.f0);

			return new MTable(
				createInput(inputFormat, new RowTypeInfo(sourceFunction.f1.getFieldTypes()), getParams()),
				sourceFunction.f1
			);
		}
		FilePath filePath = getFilePath();
		LocalOperator <?> selected;
		TableSchema schema;
		try {
			selected = AkUtils.selectPartitionLocalOp(filePath, getPartitions(), null);
		} catch (IOException e) {
			throw new AkIllegalOperatorParameterException("Could not find the path: " + filePath.getPathStr());
		}
		final String[] colNames = selected.getColNames();
		final ParquetClassLoaderFactory localFactory = factory;
		List <Row> res = MTableUtil.flatMapWithMultiThreads(selected.getOutputTable(), getParams(),
			new MTableUtil.FlatMapFunction() {
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
		return new MTable(res, schema);
	}
}
