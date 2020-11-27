package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommAdapter;
import com.alibaba.alink.operator.common.recommendation.RecommAdapterMT;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.shared.HasNumThreads;

/**
 * Base recommendation stream op.
 */
public class BaseRecommStreamOp<T extends BaseRecommStreamOp <T>> extends StreamOperator <T> {

	private static final long serialVersionUID = 5293170481337594373L;
	private final BatchOperator model;

	/**
	 * (modelScheme, dataSchema, params) -> RecommKernel
	 */
	private final FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder;
	private final RecommType recommType;

	public BaseRecommStreamOp(
		BatchOperator model,
		FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
		RecommType recommType,
		Params params) {

		super(params);
		this.model = model;
		this.recommKernelBuilder = recommKernelBuilder;
		this.recommType = recommType;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		TableSchema modelSchema = this.model.getSchema();

		try {
			DataBridge modelDataBridge = DirectReader.collect(model);
			DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);
			RecommKernel recommKernel
				= this.recommKernelBuilder.apply(modelSchema, in.getSchema(), this.getParams(), this.recommType);

			DataStream <Row> resultRows;

			final boolean isStreamPredictMultiThread = AlinkGlobalConfiguration.isStreamPredictMultiThread();

			if (!isStreamPredictMultiThread
				|| !getParams().contains(HasNumThreads.NUM_THREADS)
				|| getParams().get(HasNumThreads.NUM_THREADS) <= 1) {

				resultRows = in.getDataStream().map(new RecommAdapter(recommKernel, modelSource));
			} else {
				resultRows = in.getDataStream().flatMap(
					new RecommAdapterMT(recommKernel, modelSource, getParams().get(HasNumThreads.NUM_THREADS))
				);
			}

			TableSchema outputSchema = recommKernel.getOutputSchema();

			this.setOutput(resultRows, outputSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}
