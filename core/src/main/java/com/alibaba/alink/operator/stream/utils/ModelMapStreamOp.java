package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.mapper.ModelMapperAdapterMT;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.common.stream.model.PredictProcess;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.io.AkSourceParams;
import com.alibaba.alink.params.io.ModelFileSinkParams;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.HasModelFilePath;

import java.io.IOException;
import java.util.List;

/**
 * *
 */
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, opType = OpType.BATCH, desc = PortDesc.PREDICT_INPUT_MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA),
	@PortSpec(value = PortType.MODEL_STREAM, isOptional = true, desc = PortDesc.PREDICT_INPUT_MODEL_STREAM)
})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
@Internal
public class ModelMapStreamOp<T extends ModelMapStreamOp <T>> extends StreamOperator <T>
	implements ModelStreamScanParams <T>, HasModelFilePath <T> {

	private static final long serialVersionUID = -6591412871091394859L;
	protected final BatchOperator <?> model;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	protected final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapStreamOp(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder, Params params) {
		super(params);

		this.model = null;
		this.mapperBuilder = mapperBuilder;
	}

	public ModelMapStreamOp(BatchOperator <?> model,
							TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
							Params params) {
		super(params);

		this.model = model;
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		StreamOperator <?> inputData = inputs[0];

		StreamOperator <?> inputModelStream = inputs.length > 1 ? inputs[1] : null;

		try {

			Tuple2 <DataBridge, TableSchema> dataBridge = createDataBridge(
				getParams().get(ModelFileSinkParams.MODEL_FILE_PATH),
				model
			);

			final ModelMapper mapper = mapperBuilder.apply(dataBridge.f1, inputData.getSchema(),
				getParams());

			DataStream <Row> resultRows = calcResultRows(dataBridge.f0, dataBridge.f1, inputData, inputModelStream,
				mapper, getParams(), getMLEnvironmentId(), mapperBuilder);

			TableSchema resultSchema = mapper.getOutputSchema();
			setOutput(resultRows, resultSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static final class FilePathDataBridge implements DataBridge {
		private final FilePath filePath;

		private FilePathDataBridge(FilePath filePath) {
			this.filePath = filePath;
		}

		@Override
		public List <Row> read(FilterFunction <Row> filter) {
			try {
				return AkUtils.readFromPath(filePath, filter).f1;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static DataStream <Row> calcResultRows(DataBridge modelDataBridge, TableSchema modelSchema,
												  StreamOperator <?> input_data,
												  StreamOperator <?> input_model_stream,
												  ModelMapper mapper, Params params, Long mlEnvironmentId,
												  TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder) {

		final DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);

		DataStream <Row> modelStream = null;
		TableSchema modelStreamSchema = null;

		if (ModelStreamUtils.useModelStreamFile(params)) {
			StreamOperator <?> modelStreamOp = new ModelStreamFileSourceStreamOp()
				.setFilePath(FilePath.deserialize(params.get(ModelStreamScanParams.MODEL_STREAM_FILE_PATH)))
				.setScanInterval(params.get(ModelStreamScanParams.MODEL_STREAM_SCAN_INTERVAL))
				.setStartTime(params.get(ModelStreamScanParams.MODEL_STREAM_START_TIME))
				.setSchemaStr(TableUtil.schema2SchemaStr(modelSchema))
				.setMLEnvironmentId(mlEnvironmentId);
			modelStreamSchema = modelStreamOp.getSchema();
			modelStream = modelStreamOp.getDataStream();
		}

		if (null != input_model_stream) {
			StreamOperator <?> localModelStreamOp = input_model_stream;

			if (modelStream == null) {
				modelStreamSchema = localModelStreamOp.getSchema();
				modelStream = localModelStreamOp.getDataStream();
			} else {
				localModelStreamOp = localModelStreamOp.select(modelStreamSchema.getFieldNames());
				modelStream = modelStream.union(localModelStreamOp.getDataStream());
			}
		}

		if (modelStream != null) {
			return input_data
				.getDataStream()
				.connect(ModelStreamUtils.broadcastStream(modelStream))
				.flatMap(
					new PredictProcess(
						modelSchema, input_data.getSchema(), params, mapperBuilder, modelDataBridge,
						ModelStreamUtils.findTimestampColIndexWithAssertAndHint(modelStreamSchema),
						ModelStreamUtils.findCountColIndexWithAssertAndHint(modelStreamSchema)
					)
				);
		} else if (params.get(ModelMapperParams.NUM_THREADS) <= 1) {
			return input_data
				.getDataStream()
				.map(new ModelMapperAdapter(mapper, modelSource));

		} else {
			return input_data.getDataStream().flatMap(
				new ModelMapperAdapterMT(mapper, modelSource, params.get(ModelMapperParams.NUM_THREADS)));
		}
	}

	public static Tuple2 <DataBridge, TableSchema> createDataBridge(String modelFilePath, BatchOperator <?> model)
		throws IOException {

		if (modelFilePath == null && model == null) {
			throw new IllegalArgumentException("One of model or modelFilePath should be set.");
		}

		if (model != null && !(model instanceof AkSourceBatchOp || model instanceof MemSourceBatchOp)) {
			return Tuple2.of(DirectReader.collect(model), model.getSchema());
		}

		FilePath modelFile;

		if (model != null) {
			if (model instanceof MemSourceBatchOp) {
				final MTable mt = ((MemSourceBatchOp) model).getMt();

				return Tuple2.of(new MTableDataBridge(mt), mt.getSchema());
			} else {
				modelFile = FilePath.deserialize(model.getParams().get(AkSourceParams.FILE_PATH));
			}
		} else {
			modelFile = FilePath.deserialize(modelFilePath);
		}

		if (!modelFile.getFileSystem().exists(modelFile.getPath())) {
			throw new IllegalArgumentException(
				"When use model file path, the model should be sink first. "
					+ "If using pipeline model, it should be save model first.");
		}

		return Tuple2.of(
			new FilePathDataBridge(modelFile),
			TableUtil.schemaStr2Schema(AkUtils.getMetaFromPath(modelFile).schemaStr)
		);
	}

	private static class MTableDataBridge implements DataBridge {
		private final MTable mt;

		public MTableDataBridge(MTable mt) {this.mt = mt;}

		@Override
		public List <Row> read(FilterFunction <Row> filter) {
			return mt.getRows();
		}
	}
}
