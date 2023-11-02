package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.utils.FlatMapLocalOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;

import java.util.function.BiFunction;

/**
 * Abstract class for a flat map {@link TransformerBase}.
 * <p>
 * A FlatMapTransformer process the instance in single input with multiple-output.
 *
 * @param <T> class type of the {@link FlatMapTransformer} implementation itself.
 */
public abstract class FlatMapTransformer<T extends FlatMapTransformer <T>>
	extends TransformerBase <T> {

	private static final long serialVersionUID = -2155940380618604038L;
	final BiFunction <TableSchema, Params, FlatMapper> flatMapperBuilder;

	protected FlatMapTransformer(BiFunction <TableSchema, Params, FlatMapper> flatMapperBuilder, Params params) {
		super(params);
		this.flatMapperBuilder = AkPreconditions.checkNotNull(flatMapperBuilder, "flatMapperBuilder can not be null");
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(new FlatMapBatchOp <>(this.flatMapperBuilder, this.params).linkFrom(input));
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		return new FlatMapStreamOp <>(this.flatMapperBuilder, this.params).linkFrom(input);
	}

	@Override
	public LocalOperator <?> transform(LocalOperator <?> input) {
		return postProcessTransformResult(new FlatMapLocalOp <>(this.flatMapperBuilder, this.params).linkFrom(input));
	}

}
