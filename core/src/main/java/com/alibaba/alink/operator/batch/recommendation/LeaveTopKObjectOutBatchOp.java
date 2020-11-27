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
import com.alibaba.alink.params.recommendation.LeaveTopKObjectOutParams;
import com.alibaba.alink.params.recommendation.Zipped2KObjectParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Leave-k-out cross validation.
 */
public class LeaveTopKObjectOutBatchOp extends BatchOperator <LeaveTopKObjectOutBatchOp>
	implements LeaveTopKObjectOutParams <LeaveTopKObjectOutBatchOp> {

	private static final long serialVersionUID = -2703896174042986392L;

	public LeaveTopKObjectOutBatchOp() {
		this(new Params());
	}

	public LeaveTopKObjectOutBatchOp(Params params) {
		super(params);
	}

	@Override
	public LeaveTopKObjectOutBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final Double testFraction = getFraction();
		final Integer testK = getK();

		int rateIdx = TableUtil.findColIndexWithAssertAndHint(in.getSchema(), this.getRateCol());

		DataSet <Tuple2 <Boolean, Row>> splits = in
			.getDataSet()
			.groupBy(TableUtil.findColIndexWithAssertAndHint(in.getSchema(), this.getGroupCol()))
			.reduceGroup(new Split(testFraction, testK, this.getRateThreshold(), rateIdx));

		DataSet <Row> train = splits.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
			private static final long serialVersionUID = -2766287446658278413L;

			@Override
			public void flatMap(Tuple2 <Boolean, Row> value, Collector <Row> out) throws Exception {
				if (value.f0) {
					out.collect(value.f1);
				}
			}
		});

		DataSet <Row> test = splits.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
			private static final long serialVersionUID = 3051286291048876503L;

			@Override
			public void flatMap(Tuple2 <Boolean, Row> value, Collector <Row> out) throws Exception {
				if (!value.f0) {
					out.collect(value.f1);
				}
			}
		});

		BatchOperator testOp =
			new DataSetWrapperBatchOp(test, in.getColNames(), in.getColTypes())
				.setMLEnvironmentId(getMLEnvironmentId());

		Zipped2KObjectBatchOp op = new Zipped2KObjectBatchOp(
			getParams().set(Zipped2KObjectParams.INFO_COLS, new String[] {getRateCol()})
		).linkFrom(testOp);

		this.setOutput(op.getDataSet(), op.getSchema());
		this.setSideOutputTables(
			new Table[] {DataSetConversionUtil.toTable(getMLEnvironmentId(), train, in.getSchema())});

		return this;
	}

	static class Split implements GroupReduceFunction <Row, Tuple2 <Boolean, Row>> {
		private static final long serialVersionUID = 5727094306089631645L;
		private Double testFraction;
		private Integer testK;
		private double threshold;
		private int rateIdx;

		public Split(Double fraction, Integer k, double threshold, int rateIdx) {
			this.testFraction = fraction;
			this.testK = k;
			this.threshold = threshold;
			this.rateIdx = rateIdx;
		}

		@Override
		public void reduce(Iterable <Row> rows, Collector <Tuple2 <Boolean, Row>> collector) {
			List <Row> list = new ArrayList <>();
			rows.forEach(list::add);
			list.sort(
				(o1, o2) -> Double.compare(
					((Number) o2.getField(rateIdx)).doubleValue(),
					((Number) o1.getField(rateIdx)).doubleValue()
				)
			);

			int takenNum = Math.min((int) Math.ceil(list.size() * testFraction), testK);

			for (int i = 0; i < list.size(); i++) {
				Row row = list.get(i);
				collector.collect(Tuple2.of(i >= takenNum || ((Number) row.getField(rateIdx)).doubleValue() < threshold,
					row));
			}
		}
	}
}
