package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.feature.BinningPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.BinningTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.feature.HasEncode.Encode;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class GroupScorecardTrainBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		//BatchOperator.setParallelism(1);
		BinningTrainBatchOp binningTrainBatchOp = new BinningTrainBatchOp()
			.setSelectedCols(lrColNames);
		binningTrainBatchOp.linkFrom(lrData);

		GroupScorecardTrainBatchOp gcTrainOp = new GroupScorecardTrainBatchOp()
			.setGroupCols(groupColNames)
			.setSelectedCols(selectedColNames)
			.setLabelCol(labelColName)
			.setMinSamplesPerLeaf(2);

		gcTrainOp.linkFrom(lrData, binningTrainBatchOp, binningTrainBatchOp);

		gcTrainOp.getOutputTable().printSchema();

		GroupScorecardPredictBatchOp predOp = new GroupScorecardPredictBatchOp()
			.setPredictionScoreCol("pred_score")
			.setPredictionDetailCol("pred_detail")
			.setCalculateScorePerFeature(true);

		predOp.linkFrom(gcTrainOp, lrData).lazyPrint();

		BatchOperator.execute();

	}

	@Test
	public void testLr() throws Exception {
		BinningTrainBatchOp binningTrainBatchOp = new BinningTrainBatchOp()
			.setSelectedCols(selectedColNames);
		binningTrainBatchOp.linkFrom(lrData);

		String binningOutColName = "ass_vector";
		BinningPredictBatchOp binningPredictBatchOp = new BinningPredictBatchOp()
			.setSelectedCols(selectedColNames)
			.setEncode(Encode.ASSEMBLED_VECTOR)
			.setOutputCols(binningOutColName);

		binningPredictBatchOp.linkFrom(binningTrainBatchOp, lrData);

		LogisticRegressionTrainBatchOp lr = new LogisticRegressionTrainBatchOp()
			.setVectorCol(binningOutColName)
			.setLabelCol(labelColName);

		lr.linkFrom(binningPredictBatchOp).print();
	}

	private static final Row[] array = new Row[] {
		Row.of(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1),
		Row.of(1.0, 2.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1),
		Row.of(2.0, 3.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1),
		Row.of(3.0, 4.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1),
		Row.of(3.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(3.0, 2.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(4.0, 3.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(4.0, 4.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1),
		Row.of(1.0, 2.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1),
		Row.of(2.0, 3.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1),
		Row.of(3.0, 4.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1),
		Row.of(3.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(3.0, 2.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(4.0, 3.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(4.0, 4.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1),
		Row.of(1.0, 2.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1),
		Row.of(2.0, 3.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1),
		Row.of(3.0, 4.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1),
		Row.of(3.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(3.0, 2.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(4.0, 3.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0),
		Row.of(4.0, 4.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0)
	};

	private static final String[] groupColNames = new String[] {"f0", "f1"};

	private static final String[] selectedColNames = new String[] {"f2", "f3", "f4", "f5", "f6", "f7"};
	private static final String[] lrColNames = new String[] {"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"};

	private static final String labelColName = "label";
	private BatchOperator <?> lrData;

	@Before
	public void init() {
		lrData = new MemSourceBatchOp(
			Arrays.asList(array), new String[] {"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "label"});
	}

}