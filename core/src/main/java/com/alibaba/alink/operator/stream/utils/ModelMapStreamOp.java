package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

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
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.mapper.ModelMapperAdapterMT;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.common.stream.model.PredictProcess;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.mapper.ModelMapperParams;

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
	implements ModelStreamScanParams <T> {

	private static final long serialVersionUID = -6591412871091394859L;
	protected final BatchOperator <?> model;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	protected final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

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

			final ModelMapper mapper = mapperBuilder.apply(model.getSchema(), inputData.getSchema(),
				getParams());

			DataStream <Row> resultRows = calcResultRows(model, inputData, inputModelStream,
				mapper, getParams(), getMLEnvironmentId(), mapperBuilder);

			TableSchema resultSchema = mapper.getOutputSchema();
			setOutput(resultRows, resultSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static DataStream <Row> calcResultRows(BatchOperator <?> model, StreamOperator <?> input_data,
												  StreamOperator <?> input_model_stream,
												  ModelMapper mapper, Params params, Long mlEnvironmentId,
												  TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder) {
		DataBridge modelDataBridge = DirectReader.collect(model);
		final DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);
		TableSchema modelSchema = model.getSchema();

		DataStream <Row> modelStream = null;
		TableSchema modelStreamSchema = null;

		if (ModelStreamUtils.useModelStreamFile(params)) {
			StreamOperator <?> modelStreamOp = new ModelStreamFileSourceStreamOp()
				.setFilePath(params.get(ModelStreamScanParams.MODEL_STREAM_FILE_PATH))
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

}
