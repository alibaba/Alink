package com.alibaba.alink.pipeline;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;

/**
 * The base class for transformer implementations.
 *
 * @param <T> The class type of the {@link TransformerBase} implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 */
public abstract class TransformerBase<T extends TransformerBase<T>>
    extends PipelineStageBase<T> implements Transformer<T> {

    public TransformerBase() {
        super();
    }

    public TransformerBase(Params params) {
        super(params);
    }

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		Preconditions.checkArgument(
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
		Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		if (tableEnvOf(input) instanceof StreamTableEnvironment) {
			TableSourceStreamOp source = new TableSourceStreamOp(input);
			if(this.params.contains(ML_ENVIRONMENT_ID)){
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return transform(source).getOutputTable();
		} else {
			TableSourceBatchOp source = new TableSourceBatchOp(input);
			if(this.params.contains(ML_ENVIRONMENT_ID)){
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return transform(source).getOutputTable();
		}
	}

    /**
     * Applies the transformer on the input batch data from BatchOperator, and returns the batch result data with
     * BatchOperator.
     *
     * @param input the input batch data from BatchOperator
     * @return the transformed batch result data
     */
    public abstract BatchOperator transform(BatchOperator input);

    /**
     * Applies the transformer on the input streaming data from StreamOperator, and returns the streaming result data
     * with StreamOperator.
     *
     * @param input the input streaming data from StreamOperator
     * @return the transformed streaming result data
     */
    public abstract StreamOperator transform(StreamOperator input);

}
