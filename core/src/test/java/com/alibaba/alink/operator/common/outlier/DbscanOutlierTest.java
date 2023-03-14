package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.outlier.DbscanOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.outlier.DbscanOutlierStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.outlier.DbscanDetectorParams;
import com.alibaba.alink.params.outlier.OutlierDetectorParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DbscanOutlierTest extends AlinkTestBase {
	String testCsv2D = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/contamination.csv";
	String schemaStr2D = "id int, f0 double, f1 double, label string";
	List <Row> testDf = Arrays.asList(
		Row.of(1, new Timestamp(117, 11, 1, 0, 0, 0, 0), 0.0, 7.0),
		Row.of(0, new Timestamp(117, 11, 2, 0, 0, 0, 0), 1.0, 6.0),
		Row.of(0, new Timestamp(117, 11, 3, 0, 0, 0, 0), 1.0, 6.0),
		Row.of(0, new Timestamp(117, 11, 4, 0, 0, 0, 0), 2.0, 5.0),
		Row.of(0, new Timestamp(117, 11, 5, 0, 0, 0, 0), 2.0, 5.0),
		Row.of(0, new Timestamp(117, 11, 6, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(0, new Timestamp(117, 11, 7, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(0, new Timestamp(117, 11, 8, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(0, new Timestamp(117, 11, 9, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(0, new Timestamp(117, 11, 10, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(0, new Timestamp(117, 11, 11, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(0, new Timestamp(117, 11, 12, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(0, new Timestamp(117, 11, 13, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(0, new Timestamp(117, 11, 14, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(0, new Timestamp(117, 11, 15, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(0, new Timestamp(117, 11, 16, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(0, new Timestamp(117, 11, 17, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(0, new Timestamp(117, 11, 18, 0, 0, 0, 0), 5.0, 2.0),
		Row.of(0, new Timestamp(117, 11, 19, 0, 0, 0, 0), 5.0, 2.0),
		Row.of(0, new Timestamp(117, 11, 20, 0, 0, 0, 0), 6.0, 1.0),
		Row.of(0, new Timestamp(117, 11, 21, 0, 0, 0, 0), 6.0, 1.0),
		Row.of(1, new Timestamp(117, 11, 22, 0, 0, 0, 0), 7.0, 0.0)
	);
	String[] testSchema = new String[] {"label", "ts", "f0", "f1"};
	TableSchema tableSchema = new TableSchema(new String[] {"f0", "f1"},
		new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE});

	@Test
	public void testBatchOp() throws Exception {
		EvalOutlierBatchOp detector = new MemSourceBatchOp(testDf, testSchema)
			.link(
				new DbscanOutlierBatchOp()
					.setFeatureCols(new String[] {"f0", "f1"})
					.setPredictionCol("outlier")
					.setPredictionDetailCol("details")
					.setEpsilon(1.0)
					.setMinPoints(5)
			)
			.select("label, details")
			.link(
				new EvalOutlierBatchOp()
					.setLabelCol("label")
					.setPredictionDetailCol("details")
					.setOutlierValueStrings("1")
			);
		OutlierMetrics metrics = detector.collectMetrics();
		double f1 = metrics.getF1();
		Assert.assertEquals(f1, 0.333, 0.001);
	}

	@Test
	public void testStreamOp() throws Exception {

		DbscanOutlierStreamOp outlier = new MemSourceStreamOp(testDf, testSchema)
			.link(
				new DbscanOutlierStreamOp()
					.setFeatureCols(new String[] {"f0", "f1"})
					.setTimeCol("ts")
					.setPredictionCol("outlier")
					.setPredictionDetailCol("details")
					.setEpsilon(1.0)
					.setMinPoints(3)
			);
		CollectSinkStreamOp sink = new CollectSinkStreamOp().linkFrom(outlier);
		StreamOperator.execute();
		List <Row> result = sink.getAndRemoveValues();
		Assert.assertEquals(result.size(), testDf.size());
	}

	@Test
	public void testModelOutlierOp() throws Exception {
		Row rowData1 = Row.of(2.0, 5.0);
		Row rowData2 = Row.of(6.0, 1.0);
		Params params = new Params()
			.set(DbscanDetectorParams.FEATURE_COLS, new String[] {"f0", "f1"})
			.set(OutlierDetectorParams.PREDICTION_COL, "outlier")
			.set(OutlierDetectorParams.PREDICTION_DETAIL_COL, "details")
			.set(DbscanDetectorParams.EPSILON, 3.0)
			.set(DbscanDetectorParams.MIN_POINTS, 3);
		DbscanModelDetector mapper = new DbscanModelDetector(tableSchema, tableSchema, params);

		BatchOperator modelData = new CsvSourceBatchOp().setFilePath(testCsv2D).setSchemaStr(schemaStr2D);

		mapper.loadModel(modelData.collect());

		Row result1 = mapper.map(rowData1);
		Row result2 = mapper.map(rowData2);
		Assert.assertEquals(result1.getField(2), false);
		Assert.assertEquals(result2.getField(2), true);
	}

}