package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkStream.AkWriter.AkCollector;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.io.dummy.DummyOutputFormat;
import com.alibaba.alink.operator.stream.sink.Export2FileSinkStreamOp.Export2FileOutputFormat;
import com.alibaba.alink.params.io.AkSinkBatchParams;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

/**
 * Sink batch op data to a file system with ak format. Ak is a file format define by alink.
 */
@IoOpAnnotation(name = "ak", ioType = IOType.SinkBatch)
@NameCn("AK文件导出")
public final class AkSinkBatchOp extends BaseSinkBatchOp <AkSinkBatchOp>
	implements AkSinkBatchParams <AkSinkBatchOp> {

	private static final long serialVersionUID = -6701780409272076102L;

	public AkSinkBatchOp() {
		this(new Params());
	}

	public AkSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public AkSinkBatchOp linkFrom(BatchOperator <?>... inputs) {
		return sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public AkSinkBatchOp sinkFrom(BatchOperator <?> in) {
		if (getPartitionCols() != null) {
			String[] partitionCols = getPartitionCols();

			final int[] partitionColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getSchema(), partitionCols);

			final String[] reservedCols = ArrayUtils.removeElements(in.getColNames(), partitionCols);
			final TypeInformation <?>[] reservedColTypes = TableUtil.findColTypes(in.getSchema(), reservedCols);
			final int[] reservedColIndices = TableUtil.findColIndices(in.getColNames(), reservedCols);

			final FilePath filePath = getFilePath();
			final AkMeta akMeta = new AkMeta(
				TableUtil.schema2SchemaStr(new TableSchema(reservedCols, reservedColTypes))
			);

			in
				.getDataSet()
				.groupBy(partitionCols)
				.reduceGroup(new GroupReduceFunction <Row, byte[]>() {
					@Override
					public void reduce(Iterable <Row> values, Collector <byte[]> out) throws IOException {
						Path root = filePath.getPath();
						BaseFileSystem <?> fileSystem = filePath.getFileSystem();

						AkCollector collector = null;
						Path localPath = null;

						for (Row row : values) {
							if (collector == null) {
								localPath = new Path(root.getPath());

								for (int partitionColIndex : partitionColIndices) {
									localPath = new Path(localPath, row.getField(partitionColIndex).toString());
								}

								fileSystem.mkdirs(localPath);

								collector = new AkStream(
									new FilePath(
										new Path(
											localPath, "0" + Export2FileOutputFormat.IN_PROGRESS_FILE_SUFFIX
										),
										fileSystem
									),
									akMeta
								).getWriter().getCollector();
							}

							collector.collect(Row.project(row, reservedColIndices));
						}

						if (collector != null) {
							collector.close();

							fileSystem.rename(
								new Path(
									localPath, "0" + Export2FileOutputFormat.IN_PROGRESS_FILE_SUFFIX
								),
								new Path(
									localPath, "0"
								)
							);
						}
					}
				})
				.output(new DummyOutputFormat <>());
		} else {
			in.getDataSet()
				.output(
					new AkUtils.AkOutputFormat(
						getFilePath(),
						new AkUtils.AkMeta(TableUtil.schema2SchemaStr(in.getSchema())),
						getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE)
				)
				.name("AkSink")
				.setParallelism(getNumFiles());
		}

		return this;
	}

}
