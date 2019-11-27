package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;

/**
 * A data source of integers of a range.
 */
public final class NumSeqSourceBatchOp extends BatchOperator <NumSeqSourceBatchOp> {

	public NumSeqSourceBatchOp(long n) {
		this(1L, n);
	}

	public NumSeqSourceBatchOp(long from, long to) {
		this(from, to, "num");
	}

	public NumSeqSourceBatchOp(long from, long to, Params params) {
		this(from, to, "num", params);
	}

	public NumSeqSourceBatchOp(long from, long to, String colName) {
		this(from, to, colName, new Params());
	}
	public NumSeqSourceBatchOp(long from, long to, String colName, Params params) {
		super(params);
		final int parallelism = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().getParallelism();
		DataSet <Row> data = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(0)
			. <Tuple1 <Integer>>flatMap(new FlatMapFunction <Integer, Tuple1 <Integer>>() {
				@Override
				public void flatMap(Integer value, Collector <Tuple1 <Integer>> out) throws Exception {
					for (int i = 0; i < parallelism; i++) {
						out.collect(Tuple1.of(i));
					}
				}
			})
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple1 <Integer>, Row>() {
				@Override
				public void mapPartition(Iterable <Tuple1 <Integer>> values, Collector <Row> out) throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					for (long i = from; i <= to; i++) {
						if (i % parallelism == taskId) {
							out.collect(Row.of(i));
						}
					}
				}
			});
		this.setOutput(data, new String[] {colName}, new TypeInformation[] {Types.LONG()});
	}

	@Override
	public NumSeqSourceBatchOp linkFrom(BatchOperator<?>... inputs) {
		throw new UnsupportedOperationException(
			"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}
