package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.AkUtils2;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.common.io.partition.SourceCollectorCreator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.HasFilePath;
import com.alibaba.alink.params.io.shared.HasPartitions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils for handling datastream.
 */
@SuppressWarnings("unchecked")
public class DataStreamUtil {

	/**
	 * Stack a datastream of rows
	 */
	public static DataStream <List <Row>> stack(DataStream <Row> input, final int size) {
		return input
			.flatMap(new RichFlatMapFunction <Row, List <Row>>() {
				private static final long serialVersionUID = -2909825492775487009L;
				transient Collector <List <Row>> collector;
				transient List <Row> buffer;

				@Override
				public void open(Configuration parameters) throws Exception {
					this.buffer = new ArrayList <>();
				}

				@Override
				public void close() throws Exception {
					if (this.buffer.size() > 0) {
						this.collector.collect(this.buffer);
						this.buffer.clear();
					}
				}

				@Override
				public void flatMap(Row value, Collector <List <Row>> out) throws Exception {
					this.collector = out;
					this.buffer.add(value);
					if (this.buffer.size() >= size) {
						this.collector.collect(this.buffer);
						this.buffer.clear();
					}
				}
			});
	}

	public static <T> void linkDummySink(DataStream <T> dataStream) {
		dataStream.addSink(new DummyTableSink <T>());
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

		StreamOperator <?> selected = AkUtils2
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
}
