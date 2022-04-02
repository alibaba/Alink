package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class KSigmaOutlier4SeriesStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 10.0, 0),
			Row.of(1, new Timestamp(2), 11.0, 0),
			Row.of(1, new Timestamp(3), 12.0, 0),
			Row.of(1, new Timestamp(4), 13.0, 0),
			Row.of(1, new Timestamp(5), 14.0, 0),
			Row.of(1, new Timestamp(6), 15.0, 0),
			Row.of(1, new Timestamp(7), 16.0, 0),
			Row.of(1, new Timestamp(8), 17.0, 0),
			Row.of(1, new Timestamp(9), 18.0, 0),
			Row.of(1, new Timestamp(10), 19.0, 0)
		);
		MemSourceStreamOp dataOp = new MemSourceStreamOp(mTableData,
			new String[] {"id", "ts", "val", "label"});

		CollectSinkStreamOp coSinkOp = dataOp.link(
			new OverCountWindowStreamOp()
				.setPartitionCols("id")
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("MTABLE_AGG_PRECEDING(ts, val) as series_data")
				.setReservedCols("id", "label")
		).link(
			new KSigmaOutlier4GroupedDataStreamOp()
				.setInputMTableCol("series_data")
				.setFeatureCol("val")
				.setOutputMTableCol("output_series")
				.setPredictionCol("pred")
		).link(
			new FlattenMTableStreamOp()
				.setSelectedCol("output_series")
				.setSchemaStr("ts TIMESTAMP, val DOUBLE, pred BOOLEAN")
		).link(
			new CollectSinkStreamOp()
		);

		StreamOperator.execute();

		List <Row> rows = coSinkOp.getAndRemoveValues();
		for (Row row : rows) {
			Assert.assertFalse((boolean) row.getField(6));
		}
	}
}