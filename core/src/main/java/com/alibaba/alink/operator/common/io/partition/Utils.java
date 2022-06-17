package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.dummy.DummyOutputFormat;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.Export2FileSinkStreamOp.Export2FileOutputFormat;
import com.alibaba.alink.params.io.HasFilePath;
import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasPartitions;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

public class Utils {

	public static Tuple2 <DataSet <Row>, TableSchema> readFromPartitionBatch(
		final Params params, final Long sessionId,
		final SourceCollectorCreator sourceCollectorCreator) throws IOException {

		return readFromPartitionBatch(params, sessionId, sourceCollectorCreator, null);
	}

	public static Tuple2 <DataSet <Row>, TableSchema> readFromPartitionBatch(
		final Params params, final Long sessionId,
		final SourceCollectorCreator sourceCollectorCreator, String[] partitionCols) throws IOException {

		final FilePath filePath = FilePath.deserialize(params.get(HasFilePath.FILE_PATH));
		final String partitions = params.get(HasPartitions.PARTITIONS);

		BatchOperator <?> selected = AkUtils
			.selectPartitionBatchOp(sessionId, filePath, partitions, partitionCols);

		final String[] colNames = selected.getColNames();

		return Tuple2.of(
			selected
				.getDataSet()
				.rebalance()
				.flatMap(new FlatMapFunction <Row, Row>() {
					@Override
					public void flatMap(Row value, Collector <Row> out) throws Exception {
						Path path = filePath.getPath();

						for (int i = 0; i < value.getArity(); ++i) {
							path = new Path(path, String.format("%s=%s", colNames[i], value.getField(i)));
						}

						sourceCollectorCreator.collect(new FilePath(path, filePath.getFileSystem()), out);
					}
				}),
			sourceCollectorCreator.schema()
		);
	}

	public static Tuple2 <DataStream <Row>, TableSchema> readFromPartitionStream(
		final Params params, final Long sessionId,
		final SourceCollectorCreator sourceCollectorCreator) throws IOException {

		return readFromPartitionStream(params, sessionId, sourceCollectorCreator, null);
	}

	public static Tuple2 <DataStream <Row>, TableSchema> readFromPartitionStream(
		final Params params, final Long sessionId,
		final SourceCollectorCreator sourceCollectorCreator, String[] partitionCols) throws IOException {

		final FilePath filePath = FilePath.deserialize(params.get(HasFilePath.FILE_PATH));
		final String partitions = params.get(HasPartitions.PARTITIONS);

		StreamOperator <?> selected = AkUtils
			.selectPartitionStreamOp(sessionId, filePath, partitions, partitionCols);

		final String[] colNames = selected.getColNames();

		return Tuple2.of(
			selected
				.getDataStream()
				.rebalance()
				.flatMap(new FlatMapFunction <Row, Row>() {
					@Override
					public void flatMap(Row value, Collector <Row> out) throws Exception {
						Path path = filePath.getPath();

						for (int i = 0; i < value.getArity(); ++i) {
							path = new Path(path, String.format("%s=%s", colNames[i], value.getField(i)));
						}

						sourceCollectorCreator.collect(new FilePath(path, filePath.getFileSystem()), out);
					}
				}),
			sourceCollectorCreator.schema()
		);
	}

	public static void partitionAndWriteFile(
		BatchOperator <?> input, SinkCollectorCreator sinkCollectorCreator, Params params) {

		TableSchema schema = input.getSchema();

		String[] partitionCols = params.get(HasPartitionColsDefaultAsNull.PARTITION_COLS);
		final int[] partitionColIndices = TableUtil.findColIndicesWithAssertAndHint(schema, partitionCols);
		final String[] reservedCols = ArrayUtils.removeElements(schema.getFieldNames(), partitionCols);
		final int[] reservedColIndices = TableUtil.findColIndices(schema.getFieldNames(), reservedCols);
		final FilePath localFilePath = FilePath.deserialize(params.get(HasFilePath.FILE_PATH));

		input
			.getDataSet()
			.groupBy(partitionCols)
			.reduceGroup(new GroupReduceFunction <Row, byte[]>() {
				@Override
				public void reduce(Iterable <Row> values, Collector <byte[]> out) throws IOException {
					Path root = localFilePath.getPath();
					BaseFileSystem <?> fileSystem = localFilePath.getFileSystem();

					Collector <Row> collector = null;
					Path localPath = null;

					for (Row row : values) {
						if (collector == null) {
							localPath = new Path(root.getPath());

							for (int partitionColIndex : partitionColIndices) {
								localPath = new Path(localPath, row.getField(partitionColIndex).toString());
							}

							fileSystem.mkdirs(localPath);

							collector = sinkCollectorCreator.createCollector(new FilePath(
								new Path(
									localPath, "0" + Export2FileOutputFormat.IN_PROGRESS_FILE_SUFFIX
								),
								fileSystem
							));
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
	}
}
