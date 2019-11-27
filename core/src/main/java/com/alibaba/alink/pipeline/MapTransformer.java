package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;

import java.util.function.BiFunction;

/**
 * Abstract class for a flat map {@link TransformerBase}.
 * <p>
 * A FlatMapTransformer process the instance in single input with single-output or multiple-output.
 *
 * @param <T> class type of the {@link MapTransformer} implementation itself.
 */
public abstract class MapTransformer<T extends MapTransformer <T>>
		extends TransformerBase<T> implements LocalPredictable {

	private final BiFunction<TableSchema, Params, Mapper> mapperBuilder;

	protected MapTransformer(BiFunction<TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = Preconditions.checkNotNull(mapperBuilder, "mapperBuilder can not be null");
	}

	@Override
	public BatchOperator transform(BatchOperator input) {
		return new MapBatchOp(this.mapperBuilder, this.params).linkFrom(input);
	}

	@Override
	public StreamOperator transform(StreamOperator input) {
		return new MapStreamOp(this.mapperBuilder, this.params).linkFrom(input);
	}

	@Override
	public LocalPredictor getLocalPredictor(TableSchema inputSchema) {
		return new LocalPredictor(this.mapperBuilder.apply(inputSchema, this.getParams()));
	}

}
