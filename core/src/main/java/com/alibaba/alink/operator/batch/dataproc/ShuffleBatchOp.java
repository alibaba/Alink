package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * shuffle data.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("打乱数据顺序")
@NameEn("Data Shuffling")
public final class ShuffleBatchOp extends BatchOperator <ShuffleBatchOp> {
	private static final long serialVersionUID = 4849933592970017744L;

	public ShuffleBatchOp() {
		this(new Params());
	}

	public ShuffleBatchOp(Params params) {
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
	public ShuffleBatchOp linkFrom(BatchOperator <?>... inputs) {
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
			.mapPartition(new MapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 2542930634280452259L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
					List <Row> cached = new ArrayList <>();
					values.forEach(cached::add);
					Collections.shuffle(cached);
					cached.forEach(out::collect);
				}
			})
			.name("shuffle");

		setOutput(rows, input.getSchema());
		return this;
	}
}
