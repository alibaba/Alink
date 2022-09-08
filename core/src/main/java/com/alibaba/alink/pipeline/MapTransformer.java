package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;

import java.util.function.BiFunction;

/**
 * Abstract class for a map {@link TransformerBase}.
 * <p>
 * A MapTransformer process the instance in single input with single-output.
 *
 * @param <T> class type of the {@link MapTransformer} implementation itself.
 */
public abstract class MapTransformer<T extends MapTransformer <T>>
	extends TransformerBase <T> implements LocalPredictable {

	private static final long serialVersionUID = -2155940380618604038L;
	final BiFunction <TableSchema, Params, Mapper> mapperBuilder;

	protected MapTransformer(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = AkPreconditions.checkNotNull(mapperBuilder, "mapperBuilder can not be null");
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(new MapBatchOp <>(this.mapperBuilder, this.params).linkFrom(input));
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		return new MapStreamOp <>(this.mapperBuilder, this.params).linkFrom(input);
	}

	@Override
	public LocalOperator <?> transform(LocalOperator <?> input) {
		return postProcessTransformResult(new MapLocalOp <>(this.mapperBuilder, this.params).linkFrom(input));
	}

	@Override
	public LocalPredictor collectLocalPredictor(TableSchema inputSchema) {
		Mapper mapper = this.mapperBuilder.apply(inputSchema, this.getParams());
		mapper.open();
		return new LocalPredictor(mapper);
	}

}
