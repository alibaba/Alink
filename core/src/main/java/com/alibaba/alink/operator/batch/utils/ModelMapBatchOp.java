package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * *
 *
 */
public class ModelMapBatchOp<T extends ModelMapBatchOp<T>> extends BatchOperator<T> {

	private static final String BROADCAST_MODEL_TABLE_NAME = "broadcastModelTable";

	/**
	 *  (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction<TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapBatchOp(TriFunction<TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
						   Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator<?>... inputs) {
		checkOpSize(2, inputs);

		try {
			BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(BROADCAST_MODEL_TABLE_NAME);
			ModelMapper mapper = this.mapperBuilder.apply(
					inputs[0].getSchema(),
					inputs[1].getSchema(),
					this.getParams());
			DataSet<Row> modelRows = inputs[0].getDataSet().rebalance();
			DataSet<Row> resultRows = inputs[1].getDataSet()
					.map(new ModelMapperAdapter(mapper, modelSource))
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);

			TableSchema outputSchema = mapper.getOutputSchema();
			this.setOutput(resultRows, outputSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
