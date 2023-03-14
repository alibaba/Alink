package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.MTableUtil.GroupFunction;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.stream.sink.Export2FileOutputFormat;
import com.alibaba.alink.params.io.HasFilePath;
import com.alibaba.alink.params.io.shared.HasPartitionColsDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasPartitions;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.List;

public class LocalUtils {
	public static Tuple2 <List <Row>, TableSchema> readFromPartitionLocal(
		final Params params, final SourceCollectorCreator sourceCollectorCreator) throws IOException {

		return readFromPartitionLocal(params, sourceCollectorCreator, null);
	}

	public static Tuple2 <List <Row>, TableSchema> readFromPartitionLocal(
		final Params params, final SourceCollectorCreator sourceCollectorCreator, String[] partitionCols)
		throws IOException {

		final FilePath filePath = FilePath.deserialize(params.get(HasFilePath.FILE_PATH));
		final String partitions = params.get(HasPartitions.PARTITIONS);

		LocalOperator <?> selected = AkUtils.selectPartitionLocalOp(filePath, partitions, partitionCols);

		final String[] colNames = selected.getColNames();

		List <Row> res = MTableUtil.flatMapWithMultiThreads(selected.getOutputTable(), params,
			new MTableUtil.FlatMapFunction() {
				@Override
				public void flatMap(Row row, Collector <Row> collector) throws Exception {
					Path path = filePath.getPath();

					for (int i = 0; i < row.getArity(); ++i) {
						path = new Path(path, String.format("%s=%s", colNames[i], row.getField(i)));
					}

					sourceCollectorCreator.collect(new FilePath(path, filePath.getFileSystem()), collector);
				}
			});

		return Tuple2.of(res, sourceCollectorCreator.schema());
	}

	public static void partitionAndWriteFile(
		LocalOperator <?> input, SinkCollectorCreator sinkCollectorCreator, Params params) {

		TableSchema schema = input.getSchema();

		String[] partitionCols = params.get(HasPartitionColsDefaultAsNull.PARTITION_COLS);
		final int[] partitionColIndices = TableUtil.findColIndicesWithAssertAndHint(schema, partitionCols);
		final String[] reservedCols = ArrayUtils.removeElements(schema.getFieldNames(), partitionCols);
		final int[] reservedColIndices = TableUtil.findColIndices(schema.getFieldNames(), reservedCols);
		final FilePath localFilePath = FilePath.deserialize(params.get(HasFilePath.FILE_PATH));

		MTableUtil.groupFunc(input.getOutputTable(), partitionCols, new GroupFunction() {
			@Override
			public void calc(List <Row> values, Collector <Row> out) {
				try {
					Path root = localFilePath.getPath();
					BaseFileSystem <?> fileSystem = localFilePath.getFileSystem();

					Collector <Row> collector = null;
					Path localPath = null;

					localPath = new Path(root.getPath());

					for (int partitionColIndex : partitionColIndices) {
						localPath = new Path(localPath, values.get(0).getField(partitionColIndex).toString());
					}

					fileSystem.mkdirs(localPath);

					collector = sinkCollectorCreator.createCollector(new FilePath(
						new Path(
							localPath, "0" + Export2FileOutputFormat.IN_PROGRESS_FILE_SUFFIX
						),
						fileSystem
					));

					for (Row row : values) {
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
				} catch (IOException e) {
					throw new AkIllegalDataException("Fail to create partition directories or write files.",e);
				} 
			}
		});

	}
}
