package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextTrainBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

		Row[] array = new Row[] {
			Row.of("1.0 1.0 1.0 1.0", "pos"),
			Row.of("1.0 1.0 0.0 1.0", "pos"),
			Row.of("1.0 0.0 1.0 1.0", "pos"),
			Row.of("1.0 0.0 1.0 1.0", "pos"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg")
		};

		Row[] arrayPre = new Row[] {
			Row.of("1.0 1.0 1.0 1.0", "pos"),
			Row.of("1.0 1.0 0.0 1.0", "pos"),
			Row.of("1.0 0.0 1.0 1.0", "pos"),
			Row.of("1.0 0.0 1.0 1.0", "pos"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg"),
			Row.of("0.0 1.0 1.0 0.0", "neg")
		};

		//the above instance is more complex.
		MemSourceBatchOp bData = new MemSourceBatchOp(Arrays.asList(array),
			new String[] {"vec", "labels"});
		MemSourceBatchOp bDataPre = new MemSourceBatchOp(Arrays.asList(arrayPre),
			new String[] {"vec", "labels"});

		String vecColName = "vec";
		String bLabelName = "labels";

		NaiveBayesTextTrainBatchOp op = new NaiveBayesTextTrainBatchOp()
			.setVectorCol(vecColName)
			.setLabelCol(bLabelName)
			.setModelType("Multinomial")
			.setSmoothing(0.5);

		NaiveBayesTextTrainBatchOp bModel = op.linkFrom(bData);

		// predict
		NaiveBayesTextPredictBatchOp bPredictor = new NaiveBayesTextPredictBatchOp()
			.setPredictionCol("pred")
			.setVectorCol("vec");

		List <Row> res = bPredictor.linkFrom(bModel, bData).select(new String[] {"labels", "pred"}).collect();
		System.out.println(bModel.getModelInfoBatchOp().collectModelInfo().toString());
		for (Row row : res) {
			Assert.assertEquals(row.getField(0), row.getField(1));
		}
	}

}