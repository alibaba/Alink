package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.timeseries.DeepARPredictStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DeepARPredictStreamOpTest {

	@Test
	public void testDeepARTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-05 00:00:00"), 100.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "id int, ts timestamp, series double");

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "id int, ts timestamp, series double");

		DeepARTrainBatchOp deepARTrainBatchOp = new DeepARTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(2)
			.setStride(1)
			.linkFrom(memSourceBatchOp);

		OverCountWindowStreamOp overCountWindowStreamOp = new OverCountWindowStreamOp()
			.setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")
			.setTimeCol("ts")
			.setPrecedingRows(2);

		DeepARPredictStreamOp deepARPredictStreamOp = new DeepARPredictStreamOp(deepARTrainBatchOp)
			.setPredictNum(2)
			.setPredictionCol("pred")
			.setValueCol("mtable_agg_series");

		deepARPredictStreamOp
			.linkFrom(
				overCountWindowStreamOp
					.linkFrom(memSourceStreamOp)
					.filter("ts = TO_TIMESTAMP('2021-11-05 00:00:00')")
			)
			.print();

		StreamOperator.execute();
	}

}