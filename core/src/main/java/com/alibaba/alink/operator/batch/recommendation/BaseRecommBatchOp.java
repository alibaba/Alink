package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommAdapter;
import com.alibaba.alink.operator.common.recommendation.RecommAdapterMT;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.shared.HasNumThreads;

/**
 * *
 */
public class BaseRecommBatchOp<T extends BaseRecommBatchOp <T>> extends BatchOperator <T> {

	private static final String BROADCAST_MODEL_TABLE_NAME = "broadcastModelTable";
	private static final long serialVersionUID = -5664225520926927756L;

	/**
	 * (modelScheme, dataSchema, params) -> RecommKernel
	 */
	private final FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder;
	private final RecommType recommType;

	public BaseRecommBatchOp(
		FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
		RecommType recommType,
		Params params) {

		super(params);
		this.recommKernelBuilder = recommKernelBuilder;
		this.recommType = recommType;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(BROADCAST_MODEL_TABLE_NAME);
			RecommKernel recommKernel = this.recommKernelBuilder.apply(
				inputs[0].getSchema(),
				inputs[1].getSchema(),
				this.getParams(),
				this.recommType);

			DataSet <Row> modelRows = inputs[0].getDataSet().rebalance();
			DataSet <Row> resultRows;

			final boolean isBatchPredictMultiThread = AlinkGlobalConfiguration.isBatchPredictMultiThread();

			if (!isBatchPredictMultiThread
				|| !getParams().contains(HasNumThreads.NUM_THREADS)
				|| getParams().get(HasNumThreads.NUM_THREADS) <= 1) {

				resultRows = inputs[1].getDataSet()
					.map(new RecommAdapter(recommKernel, modelSource))
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
			} else {
				resultRows = inputs[1].getDataSet()
					.flatMap(
						new RecommAdapterMT(recommKernel, modelSource, getParams().get(HasNumThreads.NUM_THREADS))
					)
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
			}

			TableSchema outputSchema = recommKernel.getOutputSchema();

			this.setOutput(resultRows, outputSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
