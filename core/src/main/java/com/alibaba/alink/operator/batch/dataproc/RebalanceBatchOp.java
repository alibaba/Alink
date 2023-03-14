package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.Random;

/**
 * Rebalance data.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("数据Rebalance")
@NameEn("Data Rebalance")
public final class RebalanceBatchOp extends BatchOperator <RebalanceBatchOp> {
	private static final long serialVersionUID = -4236329417415800780L;

	public RebalanceBatchOp() {
		this(new Params());
	}

	public RebalanceBatchOp(Params params) {
		super(params);
	}

	/**
	 * KeySelector must be static inner class, since they are not cleaned by Flink.
	 */
	private static class ConstantIntValueKeySelector implements KeySelector <Row, Integer> {
		private static final long serialVersionUID = 2565462725031975973L;

		private final Integer constant;

		public ConstantIntValueKeySelector(Integer constant) {
			this.constant = constant;
		}

		@Override
		public Integer getKey(Row value) throws Exception {
			return constant;
		}
	}

	@Override
	public RebalanceBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> input = inputs[0];
		setMLEnvironmentId(input.getMLEnvironmentId());
		DataSet <Row> rows = input.getDataSet();

		rows = rows.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 1113996575491764680L;
				private final Random random = new Random();

				@Override
				public int partition(Integer key, int numPartitions) {
					return random.nextInt(numPartitions);
				}
			}, new ConstantIntValueKeySelector(0))
			.name("rebalance");

		setOutput(rows, input.getSchema());
		return this;
	}
}
