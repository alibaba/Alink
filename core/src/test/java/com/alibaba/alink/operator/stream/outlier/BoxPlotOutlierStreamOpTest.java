package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class BoxPlotOutlierStreamOpTest extends AlinkTestBase {
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

		MemSourceStreamOp dataOp = new MemSourceStreamOp(mTableData, new String[] {"id", "ts", "val", "label"});

		BoxPlotOutlierStreamOp outlierOp = new BoxPlotOutlierStreamOp()
			.setGroupCols("id")
			.setTimeCol("ts")
			.setPrecedingRows(3)
			.setFeatureCol("val")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		CollectSinkStreamOp coSinkOp = new CollectSinkStreamOp();

		dataOp.link(outlierOp).link(coSinkOp);

		StreamOperator.execute();

		List <Row> rows = coSinkOp.getAndRemoveValues();
		for (Row row : rows) {
			Assert.assertFalse((boolean) row.getField(4));
		}

	}
}