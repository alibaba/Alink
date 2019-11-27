package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;

import java.util.List;

/**
 * Abstract class for a flat map {@link ModelBase}.
 * <p>
 * A MapModel predict the instance in single input with single-output or multiple-output.
 *
 * @param <T> class type of the {@link MapModel} implementation itself.
 */
public abstract class MapModel<T extends MapModel<T>>
		extends ModelBase<T> implements LocalPredictable {

	/**
	 *  (modelScheme, dataSchema, params) -> FlatModelMapper
	 */
	final TriFunction<TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	protected MapModel(TriFunction<TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
					   Params params) {
		super(params);
		this.mapperBuilder = Preconditions.checkNotNull(mapperBuilder, "mapperBuilder can not be null");
	}

	@Override
	public BatchOperator transform(BatchOperator input) {
		return new ModelMapBatchOp(this.mapperBuilder, this.params)
				.linkFrom(BatchOperator.fromTable(this.getModelData())
					.setMLEnvironmentId(input.getMLEnvironmentId()), input);
	}

	@Override
	public StreamOperator transform(StreamOperator input) {
		return new ModelMapStreamOp(BatchOperator.fromTable(this.getModelData())
			.setMLEnvironmentId(input.getMLEnvironmentId()), this.mapperBuilder,
			this.params).linkFrom(input);
	}

	@Override
	public LocalPredictor getLocalPredictor(TableSchema inputSchema) throws Exception {
		List<Row> modelRows = DataSetConversionUtil.fromTable(getMLEnvironmentId(), this.modelData).collect();
		ModelMapper mapper = mapperBuilder.apply(modelData.getSchema(), inputSchema, this.getParams());
		mapper.loadModel(modelRows);
		return new LocalPredictor(mapper);
	}
}
