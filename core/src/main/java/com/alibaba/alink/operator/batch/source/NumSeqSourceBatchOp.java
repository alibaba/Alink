package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.params.io.NumSeqSourceParams;

/**
 * A data source of integers of a range.
 */
@IoOpAnnotation(name = "num_seq", ioType = IOType.SourceBatch)
public final class NumSeqSourceBatchOp extends BaseSourceBatchOp <NumSeqSourceBatchOp>
	implements NumSeqSourceParams <NumSeqSourceBatchOp> {

	private static final String DEFAULT_OUTPUT_COL_NAME = "num";
	private static final long serialVersionUID = 6536810460146700008L;

	public NumSeqSourceBatchOp() {
		this(null);
	}

	public NumSeqSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(NumSeqSourceBatchOp.class), params);

		if (!getParams().contains(NumSeqSourceParams.OUTPUT_COL)) {
			setOutputCol(DEFAULT_OUTPUT_COL_NAME);
		}
	}

	@Deprecated
	public NumSeqSourceBatchOp(long n) {
		this(1L, n);
	}

	@Deprecated
	public NumSeqSourceBatchOp(long from, long to) {
		this(from, to, DEFAULT_OUTPUT_COL_NAME);
	}

	@Deprecated
	public NumSeqSourceBatchOp(long from, long to, Params params) {
		this(from, to, DEFAULT_OUTPUT_COL_NAME, params);
	}

	@Deprecated
	public NumSeqSourceBatchOp(long from, long to, String colName) {
		this(from, to, colName, new Params());
	}

	@Deprecated
	public NumSeqSourceBatchOp(long from, long to, String colName, Params params) {
		this(
			params
				.clone()
				.set(NumSeqSourceParams.FROM, from)
				.set(NumSeqSourceParams.TO, to)
				.set(NumSeqSourceParams.OUTPUT_COL, colName)
		);
	}

	@Override
	protected Table initializeDataSource() {
		final long from = getFrom();
		final long to = getTo();
		final String colName = getOutputCol();
		final int parallelism = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getExecutionEnvironment()
			.getParallelism();

		DataSet <Row> data = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getExecutionEnvironment()
			.fromElements(0)
			. <Tuple1 <Integer>>flatMap(new FlatMapFunction <Integer, Tuple1 <Integer>>() {
				private static final long serialVersionUID = 4025060738763494851L;

				@Override
				public void flatMap(Integer value, Collector <Tuple1 <Integer>> out) {
					for (int i = 0; i < parallelism; i++) {
						out.collect(Tuple1.of(i));
					}
				}
			})
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = 2199481887391477466L;

				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple1 <Integer>, Row>() {
				private static final long serialVersionUID = 4854657588340027916L;

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

		return DataSetConversionUtil.toTable(
			getMLEnvironmentId(),
			data,
			new String[] {colName},
			new TypeInformation <?>[] {Types.LONG}
		);
	}

}
