package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.modelstream.FileModelStreamSink;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.params.io.AppendModelStreamFileSinkParams;

import java.io.IOException;
import java.sql.Timestamp;

@IoOpAnnotation(name = "append_model_stream", ioType = IOType.SinkBatch)
@InputPorts(values = {@PortSpec(PortType.MODEL)})
@NameCn("模型流导出")
@NameEn("Append Model Stream File Sink")
public class AppendModelStreamFileSinkBatchOp extends BaseSinkBatchOp <AppendModelStreamFileSinkBatchOp>
	implements AppendModelStreamFileSinkParams <AppendModelStreamFileSinkBatchOp> {

	public AppendModelStreamFileSinkBatchOp() {
		this(new Params());
	}

	public AppendModelStreamFileSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(AppendModelStreamFileSinkBatchOp.class), params);
	}

	@Override
	protected AppendModelStreamFileSinkBatchOp sinkFrom(BatchOperator <?> in) {
		final FilePath filePath = getFilePath();
		final Timestamp timestamp = ModelStreamUtils.createStartTime(getModelTime());
		final int numFiles = getNumFiles();
		final int numKeepModel = getNumKeepModel();
		final TableSchema schema = in.getSchema();

		final FileModelStreamSink sink = new FileModelStreamSink(
			filePath, TableUtil.schema2SchemaStr(schema)
		);

		try {
			sink.initializeGlobal();
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Error. ",e);
		}

		DataSet <Row> writtenModel = in.getDataSet()
			.map(new RichMapFunction <Row, Row>() {
				@Override
				public void open(Configuration parameters) throws Exception {
					sink.open(timestamp, getRuntimeContext().getIndexOfThisSubtask());
				}

				@Override
				public void close() throws Exception {
					sink.close();
				}

				@Override
				public Row map(Row value) throws Exception {
					sink.collect(value);
					return value;
				}
			})
			.setParallelism(numFiles);

		DataSetUtils
			.countElementsPerPartition(writtenModel)
			.sum(1)
			.output(new OutputFormat <Tuple2 <Integer, Long>>() {
				@Override
				public void configure(Configuration parameters) {
					// pass
				}

				@Override
				public void open(int taskNumber, int numTasks) throws IOException {
					// pass
				}

				@Override
				public void writeRecord(Tuple2 <Integer, Long> record) throws IOException {
					sink.finalizeGlobal(timestamp, record.f1, numFiles, numKeepModel);
				}

				@Override
				public void close() throws IOException {
					// pass
				}
			})
			.setParallelism(1);

		return this;
	}
}
