package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommAdapter;
import com.alibaba.alink.operator.common.recommendation.RecommAdapterMT;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommMapper;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.mapper.ModelMapperParams;

/**
 * *
 */

@Internal
@InputPorts(values = {@PortSpec(PortType.MODEL), @PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
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
		BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(BROADCAST_MODEL_TABLE_NAME);

		RecommMapper mapper =
			new RecommMapper(
				this.recommKernelBuilder, this.recommType,
				inputs[0].getSchema(),
				inputs[1].getSchema(), this.getParams()
			);

		DataSet <Row> modelRows = inputs[0].getDataSet().rebalance();
		DataSet <Row> resultRows;

		if (getParams().get(ModelMapperParams.NUM_THREADS) <= 1) {
			resultRows = inputs[1].getDataSet()
				.map(new RecommAdapter(mapper, modelSource))
				.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
		} else {
			resultRows = inputs[1].getDataSet()
				.flatMap(
					new RecommAdapterMT(mapper, modelSource, getParams().get(ModelMapperParams.NUM_THREADS))
				)
				.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
		}

		TableSchema outputSchema = mapper.getOutputSchema();

		this.setOutput(resultRows, outputSchema);
		return (T) this;
	}
}
