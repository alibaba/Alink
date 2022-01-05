package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.common.recommendation.Zipped2KObjectBatchOp;
import com.alibaba.alink.params.recommendation.LeaveKObjectOutParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Leave-k-out cross validation.
 */
public class LeaveKObjectOutBatchOp extends BatchOperator <LeaveKObjectOutBatchOp>
	implements LeaveKObjectOutParams <LeaveKObjectOutBatchOp> {
	private static final long serialVersionUID = 8447591038487459735L;

	public LeaveKObjectOutBatchOp() {
		this(new Params());
	}

	public LeaveKObjectOutBatchOp(Params params) {
		super(params);
	}

	@Override
	public LeaveKObjectOutBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final Double testFraction = getFraction();
		final Integer testK = getK();

		DataSet <Tuple2 <Boolean, Row>> splits = in
			.getDataSet()
			.groupBy(TableUtil.findColIndexWithAssertAndHint(in.getSchema(), this.getGroupCol()))
			.reduceGroup(new Split(testFraction, testK));

		DataSet <Row> train = splits.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
			private static final long serialVersionUID = 8194568313949873147L;

			@Override
			public void flatMap(Tuple2 <Boolean, Row> value, Collector <Row> out) {
				if (value.f0) {
					out.collect(value.f1);
				}
			}
		});

		DataSet <Row> test = splits.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
			private static final long serialVersionUID = 7652429568716812411L;

			@Override
			public void flatMap(Tuple2 <Boolean, Row> value, Collector <Row> out) {
				if (!value.f0) {
					out.collect(value.f1);
				}
			}
		});

		BatchOperator <?> testOp = new DataSetWrapperBatchOp(test, in.getColNames(), in.getColTypes())
			.setMLEnvironmentId(getMLEnvironmentId());

		Zipped2KObjectBatchOp op = new Zipped2KObjectBatchOp(getParams()).linkFrom(testOp);

		this.setOutput(op.getDataSet(), op.getSchema());
		this.setSideOutputTables(
			new Table[] {DataSetConversionUtil.toTable(getMLEnvironmentId(), train, in.getSchema())});
		return this;
	}

	private static class Split implements GroupReduceFunction <Row, Tuple2 <Boolean, Row>> {
		private static final long serialVersionUID = 9130706753665003510L;
		private final Double testFraction;
		private final Integer testK;

		public Split(Double fraction, Integer k) {
			this.testFraction = fraction;
			this.testK = k;
		}

		@Override
		public void reduce(Iterable <Row> rows, Collector <Tuple2 <Boolean, Row>> collector) {
			List <Row> list = new ArrayList <>();
			rows.forEach(list::add);

			Collections.shuffle(list);

			int size = list.size();

			int takenNum = Math.min((int) Math.ceil(size * testFraction), testK);

			for (int i = 0; i < size; i++) {
				collector.collect(Tuple2.of(i >= takenNum, list.get(i)));
			}
		}
	}
}
