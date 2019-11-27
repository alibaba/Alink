package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * *
 *
 */
public class ModelMapStreamOp<T extends ModelMapStreamOp <T>> extends StreamOperator<T> {

	private final BatchOperator model;

	/**
	 *  (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction<TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapStreamOp(BatchOperator model,
							TriFunction<TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
							Params params) {
		super(params);
		this.model = model;
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator<?>... inputs) {
		StreamOperator<?> in = checkAndGetFirst(inputs);

		TableSchema modelSchema = this.model.getSchema();

		try {
			DataBridge modelDataBridge = DirectReader.collect(model);
			DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);
			ModelMapper mapper = this.mapperBuilder.apply(modelSchema, in.getSchema(), this.getParams());
			DataStream <Row> resultRows = in.getDataStream().map(new ModelMapperAdapter(mapper, modelSource));
			TableSchema resultSchema = mapper.getOutputSchema();
			this.setOutput(resultRows, resultSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}
