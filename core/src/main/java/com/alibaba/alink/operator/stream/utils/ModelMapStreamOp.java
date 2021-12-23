package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.mapper.ModelMapperAdapterMT;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp;
import com.alibaba.alink.params.mapper.ModelMapperParams;

/**
 * *
 */
public class ModelMapStreamOp<T extends ModelMapStreamOp <T>> extends StreamOperator <T> {

	private static final long serialVersionUID = -6591412871091394859L;
	private final BatchOperator model;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapStreamOp(BatchOperator model,
							TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
							Params params) {
		super(params);
		this.model = model;
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		StreamOperator <?> in = inputs[0];

		TableSchema modelSchema = this.model.getSchema();

		try {
			DataBridge modelDataBridge = DirectReader.collect(model);
			final DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);
			final ModelMapper mapper = this.mapperBuilder.apply(modelSchema, in.getSchema(), this.getParams());
			DataStream <Row> resultRows;

			DataStream <Row> modelStream = null;
			TableSchema modelStreamSchema = null;

			if (ModelStreamUtils.useModelStreamFile(getParams())) {
				StreamOperator <?> modelStreamOp = new ModelStreamFileSourceStreamOp()
					.setFilePath(FilePath.deserialize(getParams().get(ModelMapperParams.MODEL_STREAM_FILE_PATH)))
					.setScanInterval(getParams().get(ModelMapperParams.MODEL_STREAM_SCAN_INTERVAL))
					.setStartTime(getParams().get(ModelMapperParams.MODEL_STREAM_START_TIME))
					.setSchemaStr(CsvUtil.schema2SchemaStr(modelSchema))
					.setMLEnvironmentId(getMLEnvironmentId());
				modelStreamSchema = modelStreamOp.getSchema();
				modelStream = modelStreamOp.getDataStream();
			}

			if (inputs.length > 1) {
				StreamOperator <?> localModelStreamOp = inputs[1];

				if (modelStream == null) {
					modelStreamSchema = localModelStreamOp.getSchema();
					modelStream = localModelStreamOp.getDataStream();
				} else {
					localModelStreamOp = localModelStreamOp.select(modelStreamSchema.getFieldNames());
					modelStream = modelStream.union(localModelStreamOp.getDataStream());
				}
			}

			if (modelStream != null) {
				resultRows = in
					.getDataStream()
					.connect(ModelStreamUtils.broadcastStream(modelStream))
					.flatMap(
						new ModelStreamUtils.PredictProcess(
							modelSchema, in.getSchema(), getParams(), mapperBuilder, modelDataBridge,
							ModelStreamUtils.findTimestampColIndexWithAssertAndHint(modelStreamSchema),
							ModelStreamUtils.findCountColIndexWithAssertAndHint(modelStreamSchema)
						)
					);
			} else if (getParams().get(ModelMapperParams.NUM_THREADS) <= 1) {
				resultRows = in
					.getDataStream()
					.map(new ModelMapperAdapter(mapper, modelSource));

			} else {
				resultRows = in.getDataStream().flatMap(
					new ModelMapperAdapterMT(mapper, modelSource, getParams().get(ModelMapperParams.NUM_THREADS)));
			}

			TableSchema resultSchema = mapper.getOutputSchema();
			this.setOutput(resultRows, resultSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
