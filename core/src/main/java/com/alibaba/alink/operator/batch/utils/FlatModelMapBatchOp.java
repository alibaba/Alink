package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.mapper.FlatModelMapper;
import com.alibaba.alink.common.mapper.FlatModelMapperAdapter;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * *
 *
 * @author yangxu
 */
public class FlatModelMapBatchOp<T extends FlatModelMapBatchOp <T>> extends BatchOperator <T> {

	private static final String BROADCAST_MODEL_TABLE_NAME = "broadcastModelTable";
	private static final long serialVersionUID = -4974798285811955808L;

	/**
	 * (modelScheme, dataSchema, params) -> FlatModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, FlatModelMapper> mapperBuilder;

	public FlatModelMapBatchOp(
		TriFunction <TableSchema, TableSchema, Params, FlatModelMapper> mapperBuilder,
		Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(BROADCAST_MODEL_TABLE_NAME);
			FlatModelMapper flatMapper = this.mapperBuilder.apply(
				inputs[0].getSchema(),
				inputs[1].getSchema(),
				this.getParams());
			DataSet <Row> modelRows = inputs[0].getDataSet().rebalance();
			DataSet <Row> resultRows = inputs[1].getDataSet()
				.flatMap(new FlatModelMapperAdapter(flatMapper, modelSource))
				.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);

			TableSchema outputSchema = flatMapper.getOutputSchema();
			this.setOutput(resultRows, outputSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
