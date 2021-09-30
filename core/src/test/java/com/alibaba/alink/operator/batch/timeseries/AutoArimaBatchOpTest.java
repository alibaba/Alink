package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.dataproc.GroupDataBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class AutoArimaBatchOpTest extends AlinkTestBase {

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

		MemSourceBatchOp source = new MemSourceBatchOp(mTableData, new String[] {"id", "ts", "val"});

		source.link(
			new GroupDataBatchOp()
				.setGroupCols("id")
				.setSelectedCols("ts", "val")
				.setOutputCol("data")
		).link(new AutoArimaBatchOp()
			.setValueCol("data")
			.setPredictionCol("pred")
			.setPredictNum(12)
		).print();
	}
}