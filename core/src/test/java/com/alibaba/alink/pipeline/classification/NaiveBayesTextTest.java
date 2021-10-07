package com.alibaba.alink.pipeline.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextTest extends AlinkTestBase {

	private static AlgoOperator getData(boolean isBatch) {
		Row[] array = new Row[] {
			Row.of(2, "$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", "a"),
			Row.of(1, "$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", "a"),
			Row.of(1, "$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "a"),
			Row.of(1, "$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "a"),
			Row.of(1, "$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "b"),
			Row.of(1, "$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "b"),
			Row.of(1, "$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "b"),
		};

		if (isBatch) {
			return new MemSourceBatchOp(
				Arrays.asList(array), new String[] {"weight", "svec", "vec", "labels"});
		} else {
			return new MemSourceStreamOp(
				Arrays.asList(array), new String[] {"weight", "svec", "vec", "labels"});
		}
	}

	@Test
	public void testPipelineBatch() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

		String labelName = "labels";

		NaiveBayesTextClassifier vnb
			= new NaiveBayesTextClassifier()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setVectorCol("vec")
			.setPredictionCol("predvResult")
			.setPredictionDetailCol("predvResultColName")
			.setSmoothing(0.5);

		NaiveBayesTextClassifier svnb
			= new NaiveBayesTextClassifier()
			.setModelType("Bernoulli")
			.setLabelCol(labelName)
			.setVectorCol("svec")
			.setPredictionCol("predsvResult")
			.setPredictionDetailCol("predsvResultColName")
			.setSmoothing(0.5);

		Pipeline pl = new Pipeline().add(vnb).add(svnb);

		PipelineModel model = pl.fit((BatchOperator) getData(true));
		BatchOperator result = model.transform((BatchOperator) getData(true)).select(
			new String[] {"labels", "predvResult", "predsvResult"});

		List <Row> data = result.collect();
		for (Row row : data) {
			for (int i = 1; i < 3; ++i) {
				Assert.assertEquals(row.getField(0), row.getField(i));
			}
		}
		// below is stream test code.
		model.transform((StreamOperator) getData(false)).print();
		StreamOperator.execute();
	}

	@Test
	public void testBatch() throws Exception {
		String labelName = "labels";
		NaiveBayesTextTrainBatchOp op = new NaiveBayesTextTrainBatchOp()
			.setWeightCol("weight")
			.setModelType("Multinomial")//Multinomial Bernoulli
			.setLabelCol(labelName)
			.setVectorCol("vec")
			.setSmoothing(0.5).linkFrom((BatchOperator) getData(true));

		//		op.lazyCollectModelInfo(new Consumer<NaiveBayesTextModelInfo>() {
		//			@Override
		//			public void accept(NaiveBayesTextModelInfo modelInfo) {
		//				System.out.println(modelInfo.getVectorColName());
		//				System.out.println(modelInfo.getModelType());
		//				System.out.println(modelInfo.getLabelList().length);
		//				System.out.println(modelInfo.getFeatureLabelInfo().length);
		//				System.out.println(modelInfo.getPositiveFeatureProportionPerLabel().length);
		//				System.out.println(modelInfo.toString());
		//			}
		//		});

		//		op.lazyPrint(-1);

		op.lazyPrintModelInfo();
		NaiveBayesTextPredictBatchOp predict = new NaiveBayesTextPredictBatchOp()
			.setPredictionCol("predsvResult")
			.setVectorCol("vec")
			.setPredictionDetailCol("detail");
		predict = predict.linkFrom(op, (BatchOperator) getData(true));
		predict.lazyPrint(-1);
		BatchOperator.execute();
	}

}
