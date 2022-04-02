package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.stream.model.ModelStreamFileScanner;
import com.alibaba.alink.operator.common.stream.model.ModelStreamFileScanner.ScanTask;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.params.io.ModelStreamFileSourceParams;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@IoOpAnnotation(name = "modelstream_file", ioType = IOType.SourceStream)
@InputPorts()
@OutputPorts(values = @PortSpec(value = PortType.MODEL_STREAM))
@NameCn("流式模型流输入")
public final class ModelStreamFileSourceStreamOp extends BaseSourceStreamOp <ModelStreamFileSourceStreamOp>
	implements ModelStreamFileSourceParams <ModelStreamFileSourceStreamOp> {

	private static final long serialVersionUID = 6926655915805468249L;

	public ModelStreamFileSourceStreamOp() {
		this(new Params());
	}

	public ModelStreamFileSourceStreamOp(Params parameter) {
		super(AnnotationUtils.annotatedName(ModelStreamFileSourceStreamOp.class), parameter);
	}

	@Override
	protected Table initializeDataSource() {

		final FilePath filePath = getFilePath();

		return DataStreamConversionUtil.toTable(
			getMLEnvironmentId(),
			createModelFileIdSource(
				getMLEnvironmentId(),
				filePath,
				ModelStreamUtils.createStartTime(getStartTime()),
				ModelStreamUtils.createScanIntervalMillis(getScanInterval()))
				.rebalance()
				.flatMap(new RichFlatMapFunction <Timestamp, Tuple3 <Timestamp, Long, FilePath>>() {
					@Override
					public void flatMap(Timestamp value, Collector <Tuple3 <Timestamp, Long, FilePath>> out) {
						Tuple3 <Timestamp, Long, FilePath> modelDesc = ModelStreamUtils.descModel(filePath, value);

						try {
							for (FilePath filePath : ModelStreamUtils.listModelFiles(modelDesc.f2)) {
								out.collect(Tuple3.of(modelDesc.f0, modelDesc.f1, filePath));
							}
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				})
				.rebalance()
				.flatMap(new RichFlatMapFunction <Tuple3 <Timestamp, Long, FilePath>, Row>() {
					@Override
					public void flatMap(Tuple3 <Timestamp, Long, FilePath> value, Collector <Row> out)
						throws Exception {
						Tuple2 <TableSchema, List <Row>> rows = AkUtils.readFromPath(value.f2);

						for (Row row : rows.f1) {
							out.collect(
								ModelStreamUtils
									.genRowWithIdentifierInternal(row, value.f0, value.f1));
						}
					}
				}),

			ModelStreamUtils.createSchemaWithModelStreamPrefix(
				ModelStreamUtils.createSchemaFromFilePath(filePath, getSchemaStr())
			)
		);
	}

	private static DataStream <Timestamp> createModelFileIdSource(
		Long mlEnvId, FilePath filePath, Timestamp startTime, long scanInterval) {

		return MLEnvironmentFactory
			.get(mlEnvId)
			.getStreamExecutionEnvironment()
			.createInput(new FileModelStreamSourceMonitorInputFormat(filePath, startTime, scanInterval))
			.setParallelism(1);
	}

	private static class FileModelStreamSourceInputSplit implements InputSplit {

		@Override
		public int getSplitNumber() {
			return 1;
		}
	}

	private static class FileModelStreamSourceMonitorInputFormat
		implements InputFormat <Timestamp, FileModelStreamSourceInputSplit> {

		private final FilePath filePath;
		private final Timestamp startTime;
		private final long scanInterval;

		private transient ModelStreamFileScanner fileScanner;
		private transient Iterator <Timestamp> streamSourceIterator;

		public FileModelStreamSourceMonitorInputFormat(FilePath filePath, Timestamp startTime, long scanInterval) {
			this.filePath = filePath;
			this.startTime = startTime;
			this.scanInterval = scanInterval;
		}

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return null;
		}

		@Override
		public FileModelStreamSourceInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			return new FileModelStreamSourceInputSplit[] {new FileModelStreamSourceInputSplit()};
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(FileModelStreamSourceInputSplit[] inputSplits) {
			return new InputSplitAssigner() {
				@Override
				public InputSplit getNextInputSplit(String host, int taskId) {
					return inputSplits[0];
				}

				@Override
				public void returnInputSplit(List <InputSplit> splits, int taskId) {
					splits.add(inputSplits[0]);
				}
			};
		}

		@Override
		public void open(FileModelStreamSourceInputSplit split) throws IOException {
			fileScanner = new ModelStreamFileScanner(1, 2);
			fileScanner.open();
			streamSourceIterator = fileScanner.scanToFile(
				new ScanTask(filePath, startTime),
				Time.of(scanInterval, TimeUnit.MILLISECONDS)
			);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return !streamSourceIterator.hasNext();
		}

		@Override
		public Timestamp nextRecord(Timestamp reuse) throws IOException {
			return streamSourceIterator.next();
		}

		@Override
		public void close() throws IOException {
			if (fileScanner != null) {
				fileScanner.close();
			}
		}
	}
}
