package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class LdaTrainBatchOpTest extends AlinkTestBase {
	Row[] testArray =
		new Row[] {
			Row.of(0, "a b b c c c c c c e e f f f g h k k k"),
			Row.of(1, "a b b b d e e e h h k"),
			Row.of(2, "a b b b b c f f f f g g g g g g g g g i j j"),
			Row.of(3, "a a b d d d g g g g g i i j j j k k k k k k k k k"),
			Row.of(4, "a a a b c d d d d d d d d d e e e g g j k k k"),
			Row.of(5, "a a a a b b d d d e e e e f f f f f g h i j j j j"),
			Row.of(6, "a a b d d d g g g g g i i j j k k k k k k k k k"),
			Row.of(7, "a b c d d d d d d d d d e e f g g j k k k"),
			Row.of(8, "a a a a b b b b d d d e e e e f f g h h h"),
			Row.of(9, "a a b b b b b b b b c c e e e g g i i j j j j j j j k k"),
			Row.of(10, "a b c d d d d d d d d d f f g g j j j k k k"),
			Row.of(11, "a a a a b e e e e f f f f f g h h h j")
		};

	@Test
	public void testTrainAndPredict() throws Exception {

		BatchOperator data = new TableSourceBatchOp(MLEnvironmentFactory.getDefault().createBatchTable(
			Arrays.asList(testArray),
			new String[] {"id", "libsvm"}));

		LdaTrainBatchOp online1 = new LdaTrainBatchOp()
			.setSelectedCol("libsvm")
			.setTopicNum(6)
			.setMethod("online")
			.setSubsamplingRate(1.0)
			.setOptimizeDocConcentration(true)
			.setOnlineLearningOffset(1024.0)
			.setLearningDecay(0.51)
			.setVocabSize(1 << 18)
			.setNumIter(50).linkFrom(data);
		online1.lazyCollect();
		online1.getSideOutput(0).lazyCollect();

		LdaTrainBatchOp lda = new LdaTrainBatchOp()
			.setSelectedCol("libsvm")
			.setTopicNum(6)
			.setMethod("online")
			.setSubsamplingRate(1.0)
			.setOptimizeDocConcentration(true)
			.setOnlineLearningOffset(1024.0)
			.setLearningDecay(0.51)
			.setAlpha(0.1)
			.setBeta(0.1)
			.setVocabSize(1 << 18)
			.setNumIter(50);

		LdaTrainBatchOp model = lda.linkFrom(data);
		model.getSideOutput(1).lazyCollect(new Consumer <List <Row>>() {
			@Override
			public void accept(List <Row> d) {
				assertEquals((Double) (d.get(0)).getField(0), 2.5106562177356033, 1.0);
			}
		});
		LdaModelInfoBatchOp modelInfo = model.getModelInfoBatchOp();
		modelInfo.lazyPrintModelInfo();
		LdaPredictBatchOp predictBatchOp = new LdaPredictBatchOp().setPredictionDetailCol("detail")
			.setPredictionCol("pred").setSelectedCol("libsvm");

		BatchOperator res = predictBatchOp.linkFrom(model, data);
		res.lazyCollect();
		BatchOperator.execute();
	}

	@Test
	public void testEm() throws Exception {

		BatchOperator data = new TableSourceBatchOp(MLEnvironmentFactory.getDefault().createBatchTable(
			Arrays.asList(testArray),
			new String[] {"id", "libsvm"}));

		LdaTrainBatchOp em1 = new LdaTrainBatchOp()
			.setSelectedCol("libsvm")
			.setTopicNum(6)
			.setMethod("em")
			.setSubsamplingRate(1.0)
			.setOptimizeDocConcentration(true)
			.setNumIter(50).linkFrom(data);
		em1.print();
		em1.getSideOutput(0).lazyCollect();

		LdaTrainBatchOp em2 = new LdaTrainBatchOp()
			.setSelectedCol("libsvm")
			.setTopicNum(6)
			.setMethod("em")
			.setSubsamplingRate(1.0)
			.setAlpha(0.1)
			.setBeta(0.1)
			.setOptimizeDocConcentration(true)
			.setNumIter(50).linkFrom(data);
		em2.lazyCollect();
		em2.getSideOutput(0).lazyCollect();
		LdaPredictBatchOp predictBatchOp = new LdaPredictBatchOp().setPredictionDetailCol("detail")
			.setPredictionCol("pred").setSelectedCol("libsvm");
		predictBatchOp.linkFrom(em1, data).lazyCollect();
		BatchOperator.execute();
	}

	@Test
	public void test() {
		System.out.println(1 << 18);
	}
}
