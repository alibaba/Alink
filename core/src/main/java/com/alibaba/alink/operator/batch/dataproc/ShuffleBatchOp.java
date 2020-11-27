package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * shuffle data.
 */
public final class ShuffleBatchOp extends BatchOperator <ShuffleBatchOp> {
	private static final long serialVersionUID = 4849933592970017744L;

	public ShuffleBatchOp() {
		this(new Params());
	}

	public ShuffleBatchOp(Params params) {
		super(params);
	}

	@Override
	public ShuffleBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator input = inputs[0];
		setMLEnvironmentId(input.getMLEnvironmentId());
		DataSet <Row> rows = input.getDataSet();

		rows = rows.rebalance()
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
