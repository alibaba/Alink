package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LookupValueInTimeSeriesStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {

		List <Row> mTableData = Arrays.asList(
			Row.of(new Timestamp(1), 10.0),
			Row.of(new Timestamp(2), 11.0),
			Row.of(new Timestamp(3), 12.0),
			Row.of(new Timestamp(4), 13.0),
			Row.of(new Timestamp(5), 14.0),
			Row.of(new Timestamp(6), 15.0),
			Row.of(new Timestamp(7), 16.0),
			Row.of(new Timestamp(8), 17.0),
			Row.of(new Timestamp(9), 18.0),
			Row.of(new Timestamp(10), 19.0)
		);

		MTable mtable = new MTable(mTableData, "ts timestamp, val double");

		MemSourceStreamOp source = new MemSourceStreamOp(
			new Object[][] {
				{1, new Timestamp(5), mtable}
			},
			new String[] {"id", "ts", "data"});

		CollectSinkStreamOp resultOp =
			source
				.link(new LookupValueInTimeSeriesStreamOp()
					.setTimeCol("ts")
					.setTimeSeriesCol("data")
					.setOutputCol("out")
				)
				.link(
					new CollectSinkStreamOp()
				);

		StreamOperator.execute();

		List <Row> sResult = resultOp.getAndRemoveValues();

		Assert.assertEquals(1, sResult.size());
		Assert.assertEquals(14.0000, (Double) sResult.get(0).getField(3), 10e-5);
	}
}