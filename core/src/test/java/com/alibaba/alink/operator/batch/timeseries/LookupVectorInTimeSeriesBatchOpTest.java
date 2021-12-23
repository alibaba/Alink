package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class LookupVectorInTimeSeriesBatchOpTest {
	@Test
	public void test() throws Exception {

		List<Row> mTableData = Arrays.asList(
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

		MTable mtable = new MTable(mTableData, "ts timestamp, val string");

		MemSourceBatchOp source = new MemSourceBatchOp(
			new Object[][] {
				{1, new Timestamp(5), mtable}
			},
			new String[] {"id", "ts", "data"});

		source
			.link(new LookupVectorInTimeSeriesBatchOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("data")
				.setOutputCol("out")
			)
			.print();
	}
}