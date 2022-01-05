package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LookupVectorInTimeSeriesStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {

		List <Row> mTableData = Arrays.asList(
			Row.of(new Timestamp(1), "10.0 21.0"),
			Row.of(new Timestamp(2), "11.0 22.0"),
			Row.of(new Timestamp(3), "12.0 23.0"),
			Row.of(new Timestamp(4), "13.0 24.0"),
			Row.of(new Timestamp(5), "14.0 25.0"),
			Row.of(new Timestamp(6), "15.0 26.0"),
			Row.of(new Timestamp(7), "16.0 27.0"),
			Row.of(new Timestamp(8), "17.0 28.0"),
			Row.of(new Timestamp(9), "18.0 29.0"),
			Row.of(new Timestamp(10), "19.0 30.0")
		);

		MemSourceStreamOp source = new MemSourceStreamOp(mTableData, new String[] {"ts", "val"});

		CollectSinkStreamOp resultOp =
			source.link(
					new OverCountWindowStreamOp()
						.setTimeCol("ts")
						.setPrecedingRows(5)
						.setClause("mtable_agg(ts, val) as data")
				).link(
					new ShiftStreamOp()
						.setValueCol("data")
						.setShiftNum(7)
						.setPredictNum(12)
						.setPredictionCol("predict")
				).link(
					new LookupVectorInTimeSeriesStreamOp()
						.setTimeCol("ts")
						.setTimeSeriesCol("predict")
						.setOutputCol("out")
						.setReservedCols("ts")
				)
				.link(
					new CollectSinkStreamOp()
				);

		StreamOperator.execute();

		List<Row> expectRows = Arrays.asList(
			Row.of(new Timestamp(1), null),
			Row.of(new Timestamp(2), VectorUtil.getVector("10.0 21.0")),
			Row.of(new Timestamp(3), VectorUtil.getVector("10.0 21.0")),
			Row.of(new Timestamp(4), VectorUtil.getVector("10.0 21.0")),
			Row.of(new Timestamp(5), VectorUtil.getVector("10.0 21.0")),
			Row.of(new Timestamp(6), VectorUtil.getVector("10.0 21.0")),
			Row.of(new Timestamp(7), VectorUtil.getVector("11.0 22.0")),
			Row.of(new Timestamp(8), VectorUtil.getVector("12.0 23.0")),
			Row.of(new Timestamp(9), VectorUtil.getVector("13.0 24.0")),
			Row.of(new Timestamp(10), VectorUtil.getVector("14.0 25.0"))
		);

		assertListRowEqual(expectRows, resultOp.getAndRemoveValues(), 0);
	}
}