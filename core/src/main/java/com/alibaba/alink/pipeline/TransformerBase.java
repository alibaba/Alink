package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.LocalLazyObjectsManager;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;

/**
 * The base class for transformer implementations.
 *
 * @param <T> The class type of the {@link TransformerBase} implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 */
public abstract class TransformerBase<T extends TransformerBase <T>>
	extends PipelineStageBase <T> implements Transformer <T>, HasLazyPrintTransformInfo <T> {

	private static final long serialVersionUID = 4778337757277469411L;

	public TransformerBase() {
		super();
	}

	public TransformerBase(Params params) {
		super(params);
	}

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		AkPreconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		AkPreconditions.checkArgument(
			tableEnvOf(input) == tEnv,
			"The input table is not in the specified table environment.");
		return transform(input);
	}

	/**
	 * Applies the transformer on the input table, and returns the result table.
	 *
	 * @param input the table to be transformed
	 * @return the transformed table
	 */
	public Table transform(Table input) {
		AkPreconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		if (tableEnvOf(input) instanceof StreamTableEnvironment) {
			TableSourceStreamOp source = new TableSourceStreamOp(input);
			if (this.params.contains(ML_ENVIRONMENT_ID)) {
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return transform(source).getOutputTable();
		} else {
			TableSourceBatchOp source = new TableSourceBatchOp(input);
			if (this.params.contains(ML_ENVIRONMENT_ID)) {
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return transform(source).getOutputTable();
		}
	}

	protected BatchOperator <?> postProcessTransformResult(BatchOperator <?> output) {
		LazyObjectsManager lazyObjectsManager = MLEnvironmentFactory.get(output.getMLEnvironmentId())
			.getLazyObjectsManager();
		if (get(LAZY_PRINT_TRANSFORM_DATA_ENABLED) || get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
			lazyObjectsManager.genLazyTransformResult(this).addValue(output);
			if (get(LAZY_PRINT_TRANSFORM_DATA_ENABLED)) {
				output.lazyPrint(get(LAZY_PRINT_TRANSFORM_DATA_NUM), get(LAZY_PRINT_TRANSFORM_DATA_TITLE));
			}
			if (get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
				output.lazyPrintStatistics(get(LAZY_PRINT_TRANSFORM_STAT_TITLE));
			}
		}
		return output;
	}

	protected LocalOperator <?> postProcessTransformResult(LocalOperator <?> output) {
		LocalLazyObjectsManager lazyObjectsManager =  LocalMLEnvironment.getInstance().getLazyObjectsManager();
		if (get(LAZY_PRINT_TRANSFORM_DATA_ENABLED) || get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
			lazyObjectsManager.genLazyTransformResult(this).addValue(output);
			if (get(LAZY_PRINT_TRANSFORM_DATA_ENABLED)) {
				output.lazyPrint(get(LAZY_PRINT_TRANSFORM_DATA_NUM), get(LAZY_PRINT_TRANSFORM_DATA_TITLE));
			}
			if (get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
				output.lazyPrintStatistics(get(LAZY_PRINT_TRANSFORM_STAT_TITLE));
			}
		}
		return output;
	}

	/**
	 * Applies the transformer on the input batch data from BatchOperator, and returns the batch result data with
	 * BatchOperator.
	 *
	 * @param input the input batch data from BatchOperator
	 * @return the transformed batch result data
	 */
	public abstract BatchOperator <?> transform(BatchOperator <?> input);


	/**
	 * Applies the transformer on the input streaming data from StreamOperator, and returns the streaming result data
	 * with StreamOperator.
	 *
	 * @param input the input streaming data from StreamOperator
	 * @return the transformed streaming result data
	 */
	public abstract StreamOperator <?> transform(StreamOperator <?> input);


	/**
	 * Applies the transformer on the input local data from LocalOperator, and returns the batch result data with
	 * LocalOperator.
	 *
	 * @param input the input local data from LocalOperator
	 * @return the transformed local result data
	 */
	public abstract LocalOperator <?> transform(LocalOperator <?> input);

}
