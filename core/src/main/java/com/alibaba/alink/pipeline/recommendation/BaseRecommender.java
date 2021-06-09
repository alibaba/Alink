package com.alibaba.alink.pipeline.recommendation;

import 	org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.BaseRecommBatchOp;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommMapper;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.recommendation.BaseRecommStreamOp;
import com.alibaba.alink.pipeline.LocalPredictable;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.ModelBase;

import java.util.List;

/**
 * Abstract class for a local-predictable Model {@link ModelBase}.
 * <p>
 * A MapModel predict the instance in single input with single-output or multiple-output.
 *
 * @param <T> class type of the {@link BaseRecommender} implementation itself.
 */
public abstract class BaseRecommender<T extends BaseRecommender <T>>
	extends ModelBase <T> implements LocalPredictable {

	private static final long serialVersionUID = -7172552127830712819L;
	/**
	 * (modelScheme, dataSchema, params) -> RecommKernel
	 */
	public final FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder;

	public final RecommType recommType;

	protected BaseRecommender(
		FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
		RecommType recommType,
		Params params) {

		super(params);
		this.recommKernelBuilder
			= Preconditions.checkNotNull(recommKernelBuilder, "recommKernelBuilder can not be null");
		this.recommType = recommType;
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(new BaseRecommBatchOp(this.recommKernelBuilder, this.recommType, this.params)
			.linkFrom(this.getModelData(), input));
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		return new BaseRecommStreamOp(this.getModelData(), this.recommKernelBuilder, this.recommType,
			this.params).linkFrom(input);
	}

	@Override
	public LocalPredictor collectLocalPredictor(TableSchema inputSchema) throws Exception {
		List <Row> modelRows = this.modelData.collect();
		ModelMapper mapper =
			new RecommMapper(
				this.recommKernelBuilder, this.recommType,
				modelData.getSchema(), inputSchema, this.getParams()
			);

		mapper.loadModel(modelRows);

		return new LocalPredictor(mapper);
	}
}
