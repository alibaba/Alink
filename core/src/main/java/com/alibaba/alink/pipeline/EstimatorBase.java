package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;

/**
 * The base class for estimator implementations.
 *
 * @param <E> A subclass of the {@link EstimatorBase}, used by
 *            {@link org.apache.flink.ml.api.misc.param.WithParams}
 * @param <M> class type of the {@link ModelBase} this Estimator produces.
 */
public abstract class EstimatorBase<E extends EstimatorBase <E, M>, M extends ModelBase <M>>
	extends PipelineStageBase <E> implements Estimator <E, M> {

	private static final long serialVersionUID = -6404177394828818348L;

	public EstimatorBase() {
		super();
	}

	public EstimatorBase(Params params) {
		super(params);
	}

	@Override
	public M fit(TableEnvironment tEnv, Table input) {
		Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		Preconditions.checkArgument(
			tableEnvOf(input) == tEnv,
			"The input table is not in the specified table environment.");
		return fit(input);
	}

	/**
	 * Train and produce a {@link ModelBase} which fits the records in the given {@link Table}.
	 *
	 * @param input the table with records to train the Model.
	 * @return a model trained to fit on the given Table.
	 */
	public M fit(Table input) {
		Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		if (tableEnvOf(input) instanceof StreamTableEnvironment) {
			TableSourceStreamOp source = new TableSourceStreamOp(input);
			if (this.params.contains(ML_ENVIRONMENT_ID)) {
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return fit(source);
		} else {
			TableSourceBatchOp source = new TableSourceBatchOp(input);
			if (this.params.contains(ML_ENVIRONMENT_ID)) {
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return fit(source);
		}
	}

	/**
	 * Train and produce a {@link ModelBase} which fits the records from the given {@link BatchOperator<?>}.
	 *
	 * @param input the table with records to train the Model.
	 * @return a model trained to fit on the given Table.
	 */
	public abstract M fit(BatchOperator <?> input);

	/**
	 * Train and Applies the {@link ModelBase} on the input batch data from BatchOperator,
	 * and returns the batch result data with {@link BatchOperator<?>}.
	 *
	 * @param input the input batch data from BatchOperator
	 * @return the batch result data
	 */
	public BatchOperator <?> fitAndTransform(BatchOperator <?> input) {
		return this.fit(input).transform(input);
	}

	/**
	 * Online learning and produce {@link ModelBase} series which fit the streaming records from the given {@link
	 * StreamOperator}.
	 *
	 * @param input the StreamOperator with streaming records to online train the Model series.
	 * @return the model series trained to fit on the streaming data from given StreamOperator.
	 */
	public M fit(StreamOperator <?> input) {
		throw new UnsupportedOperationException("NOT supported yet!");
	}

}
