package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DynamicTimeWarpOutlierStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 1, 0),
			Row.of(1, new Timestamp(2), 2, 0),
			Row.of(1, new Timestamp(3), 3, 0),
			Row.of(1, new Timestamp(4), 4, 0),
			Row.of(1, new Timestamp(5), 5, 0),
			Row.of(1, new Timestamp(6), 1, 0),
			Row.of(1, new Timestamp(7), 2, 0),
			Row.of(1, new Timestamp(8), 3, 0),
			Row.of(1, new Timestamp(9), 4, 0),
			Row.of(1, new Timestamp(10), 5, 0),
			Row.of(1, new Timestamp(11), 1, 0),
			Row.of(1, new Timestamp(12), 2, 0),
			Row.of(1, new Timestamp(13), 3, 0),
			Row.of(1, new Timestamp(14), 4, 0),
			Row.of(1, new Timestamp(15), 5, 0),
			Row.of(1, new Timestamp(16), 1, 0),
			Row.of(1, new Timestamp(17), 2, 0),
			Row.of(1, new Timestamp(18), 3, 0),
			Row.of(1, new Timestamp(19), 4, 0),
			Row.of(1, new Timestamp(20), 5, 0),
			Row.of(1, new Timestamp(21), 1, 0),
			Row.of(1, new Timestamp(22), 2, 0),
			Row.of(1, new Timestamp(23), 3, 0),
			Row.of(1, new Timestamp(24), 4, 0),
			Row.of(1, new Timestamp(25), 5, 0),
			Row.of(1, new Timestamp(26), 1, 0),
			Row.of(1, new Timestamp(27), 2, 0),
			Row.of(1, new Timestamp(28), 3, 0),
			Row.of(1, new Timestamp(29), 4, 0),
			Row.of(1, new Timestamp(30), 5, 0),
			Row.of(1, new Timestamp(31), 1, 0),
			Row.of(1, new Timestamp(32), 2, 0),
			Row.of(1, new Timestamp(33), 3, 0),
			Row.of(1, new Timestamp(34), 10, 0),
			Row.of(1, new Timestamp(35), 5, 0),
			Row.of(1, new Timestamp(36), 1, 0)

		);

		MemSourceStreamOp dataOp = new MemSourceStreamOp(mTableData, new String[] {"id", "ts", "val", "label"});

		StreamOperator <?> outlierOp = new DynamicTimeWarpOutlierStreamOp()
			.setGroupCols("id")
			.setTimeCol("ts")
			.setPrecedingRows(25)
			.setFeatureCol("val")
			.setPeriod(5)
			.setSeriesLength(5)
			.setHistoricalSeriesNum(4)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		//StreamOperator <?> outlierOp = new BoxPlotOutlierStreamOp()
		//	.setGroupCols("id")
		//	.setTimeCol("ts")
		//	.setPrecedingRows(20)
		//	.setFeatureCol("val")
		//	.setPredictionCol("pred")
		//	.setPredictionDetailCol("pred_detail");
		//
		//StreamOperator <?> outlierOp = new KSigmaOutlierStreamOp()
		//	.setGroupCols("id")
		//	.setTimeCol("ts")
		//	.setPrecedingRows(20)
		//	.setFeatureCol("val")
		//	.setPredictionCol("pred")
		//	.setPredictionDetailCol("pred_detail");

		dataOp.link(outlierOp).print();

		StreamOperator.execute();
	}

}