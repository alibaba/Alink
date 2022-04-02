package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.timeseries.ProphetTrainBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

public class ProphetPredictStreamOpTest {
	@Test
	public void testModel() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of("1", new Timestamp(117, 11, 1, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 2, 0, 0, 0, 0), 8.51959031601596),
				Row.of("2", new Timestamp(117, 11, 3, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 4, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 5, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 6, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 11, 7, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 8, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 9, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 10, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 11, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 12, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 13, 0, 0, 0, 0), 8.18367658262066),
			};
		String[] colNames = new String[] {"id", "ds1", "y1"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		ProphetTrainBatchOp model = new ProphetTrainBatchOp()
			.setTimeCol("ds1")
			.setValueCol("y1");
			//.setPythonCmdPath("/Library/Frameworks/Python.framework/Versions/3.7/bin/python3");

		source.link(model);

		MemSourceStreamOp streamSource = new MemSourceStreamOp(Arrays.asList(rowsData), colNames);

		OverCountWindowStreamOp over = new OverCountWindowStreamOp()
			.setTimeCol("ds1")
			.setPrecedingRows(4)
			.setClause("mtable_agg_preceding(ds1,y1) as tensor");

		ProphetPredictStreamOp streamPred = new ProphetPredictStreamOp(model)
			.setValueCol("tensor")
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		LookupVectorInTimeSeriesStreamOp valueOp = new LookupVectorInTimeSeriesStreamOp()
			.setTimeSeriesCol("pred")
			.setTimeCol("ds1")
			.setReservedCols("ds1", "tensor", "pred")
			.setOutputCol("y_hat");

		streamSource
			.link(over)
			.link(streamPred)
			.link(valueOp)
			.print();

		StreamOperator.execute();

	}

	@Test
	public void test() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of("1", new Timestamp(117, 11, 1, 0, 0, 0, 0), "9.59076113897809 9.59076113897809"),
				Row.of("1", new Timestamp(117, 11, 2, 0, 0, 0, 0), "8.51959031601596 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 3, 0, 0, 0, 0), "9.59076113897809 8.51959031601596"),
				Row.of("1", new Timestamp(117, 11, 4, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 5, 0, 0, 0, 0), "8.51959031601596 8.51959031601596"),
				Row.of("1", new Timestamp(117, 11, 6, 0, 0, 0, 0), "8.07246736935477 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 7, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 8, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 9, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 10, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 11, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 12, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 13, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
			};
		String[] colNames = new String[] {"id", "ds1", "y1"};

		MemSourceStreamOp streamSource = new MemSourceStreamOp(Arrays.asList(rowsData), colNames);

		OverCountWindowStreamOp over = new OverCountWindowStreamOp()
			.setTimeCol("ds1")
			.setPrecedingRows(4)
			.setClause("mtable_agg_preceding(ds1,y1) as tensor");

		ProphetStreamOp streamPred = new ProphetStreamOp()
			.setValueCol("tensor")
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		LookupVectorInTimeSeriesStreamOp valueOp = new LookupVectorInTimeSeriesStreamOp()
			.setTimeSeriesCol("pred")
			.setTimeCol("ds1")
			.setReservedCols("ds1", "tensor", "pred")
			.setOutputCol("y_hat");

		streamSource
			.link(over)
			.link(streamPred)
			.link(valueOp)
			//.filter("y_hat is null")
			.print();

		StreamOperator.execute();

	}

}