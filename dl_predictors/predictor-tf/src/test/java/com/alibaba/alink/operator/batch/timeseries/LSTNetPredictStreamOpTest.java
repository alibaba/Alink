package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.timeseries.LSTNetPredictStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LSTNetPredictStreamOpTest {

	@Test
	public void testLSTNetTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-06 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-07 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-08 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-09 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-10 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-11 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-12 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-13 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-14 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-15 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-16 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-17 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-18 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-19 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-20 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-21 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-22 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-23 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-24 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-25 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-26 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-27 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-28 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-29 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-30 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-12-01 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-12-02 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-12-03 00:00:00"), 200.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			data, "id int, ts timestamp, series double"
		);

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(
			data, "id int, ts timestamp, series double"
		);

		LSTNetTrainBatchOp lstNetTrainBatchOp = new LSTNetTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(24)
			.setHorizon(1)
			.linkFrom(memSourceBatchOp);

		OverCountWindowStreamOp overCountWindowStreamOp = new OverCountWindowStreamOp()
			.setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")
			.setTimeCol("ts")
			.setPrecedingRows(24);

		LSTNetPredictStreamOp lstNetPredictStreamOp = new LSTNetPredictStreamOp(lstNetTrainBatchOp)
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setReservedCols()
			.setValueCol("mtable_agg_series");

		lstNetPredictStreamOp
			.linkFrom(
				overCountWindowStreamOp
					.linkFrom(memSourceStreamOp)
					.filter("ts = TO_TIMESTAMP('2021-12-03 00:00:00')")
			)
			.print();

		StreamOperator.execute();
	}

}