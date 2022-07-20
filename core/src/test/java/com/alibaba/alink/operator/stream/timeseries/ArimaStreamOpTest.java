package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class ArimaStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 10.0),
			Row.of(1, new Timestamp(2), 11.0),
			Row.of(1, new Timestamp(3), 12.0),
			Row.of(1, new Timestamp(4), 13.0),
			Row.of(1, new Timestamp(5), 14.0),
			Row.of(1, new Timestamp(6), 15.0),
			Row.of(1, new Timestamp(7), 16.0),
			Row.of(1, new Timestamp(8), 17.0),
			Row.of(1, new Timestamp(9), 18.0),
			Row.of(1, new Timestamp(10), 19.0)
		);

		MemSourceStreamOp source = new MemSourceStreamOp(mTableData, new String[] {"id", "ts", "val"});

		CollectSinkStreamOp resultOp =
			source.link(
					new OverCountWindowStreamOp()
						.setGroupCols("id")
						.setTimeCol("ts")
						.setPrecedingRows(5)
						.setClause("mtable_agg_preceding(ts, val) as data")
				).link(
					new ArimaStreamOp()
						.setValueCol("data")
						.setOrder(1, 0, 1)
						.setPredictNum(12)
						.setPredictionCol("predict")
				).link(
					new LookupValueInTimeSeriesStreamOp()
						.setTimeCol("ts")
						.setTimeSeriesCol("predict")
						.setOutputCol("out")
				)
				.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		List <Row> sResult = resultOp.getAndRemoveValues();
		sResult.sort(new RowComparator(1));

		Assert.assertEquals(10, sResult.size());
		Assert.assertEquals(13.9992, (Double) sResult.get(4).getField(5), 10e-5);
	}
}