package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.common.MLEnvironmentFactory;

import org.apache.flink.api.java.tuple.Tuple3;

import org.junit.Test;


/**
 * Unit test of IsotonicRegression algorithm.
 */
public class IsotonicRegTrainBatchOpTest {
	private Tuple3[] tuples = new Tuple3[] {
			Tuple3.of(0.0, 0.3, 1.0),
			Tuple3.of(1.0, 0.27, 1.0),
			Tuple3.of(1.0, 0.55, 1.0),
			Tuple3.of(1.0, 0.5, 1.0),
			Tuple3.of(0.0, 0.2, 1.0),
			Tuple3.of(0.0, 0.18, 1.0),
			Tuple3.of(1.0, 0.1, 1.0),
			Tuple3.of(0.0, 0.45, 1.0),
			Tuple3.of(1.0, 0.9, 1.0),
			Tuple3.of(1.0, 0.8, 1.0),
			Tuple3.of(0.0, 0.7, 1.0),
			Tuple3.of(1.0, 0.6, 1.0),
			Tuple3.of(1.0, 0.35, 1.0),
			Tuple3.of(1.0, 0.4, 1.0),
			Tuple3.of(1.0, 0.02, 1.0)
		};
	private Tuple3[] tupleTest = new Tuple3[] {
			Tuple3.of(1.0, 0.02, 1.0),
			Tuple3.of(1.0, 0.4, 1.0),
			Tuple3.of(1.0, 0.48, 1.0),
			Tuple3.of(1.0, 0.49, 1.0),
			Tuple3.of(1.0, 0.33, 1.0),
			Tuple3.of(1.0, 0.75, 1.0),
			Tuple3.of(1.0, 1.0, 1.0),
			Tuple3.of(1.0, 1.02, 1.0),
			Tuple3.of(1.0, 0.01, 1.0),
			Tuple3.of(1.0, 0.0, 1.0)
	};
//	@Test
//	public void initListTest() throws Exception {
//		LinkedList <Tuple3 <Double, Tuple2<Double, Double>, Double>> list = op.initList(Arrays.asList(tuples));
//		double[] predict = new double[] {0.02, 0.1, 0.18, 0.2, 0.27, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.7, 0.8,
//			0.9};
//		Assert.assertEquals(list.size(), 15);
//		for (int i = 0; i < predict.length; i++) {
//			Assert.assertEquals(list.get(i).f1.f0, predict[i], 0.01);
//		}
//	}
//
//	@Test
//	public void updateListTest() {
//		LinkedList <Tuple3 <Double, Tuple2<Double, Double>, Double>> list = op.initList(Arrays.asList(tuples));
//		op.updateList(list);
//		Assert.assertEquals(list.size(), 4);
//		Tuple3 <Double, Double[], Double>[] predict = new Tuple3[] {Tuple3.of(3.0, new Double[] {0.02, 0.3}, 6.0),
//			Tuple3.of(2.0, new Double[] {0.35, 0.45}, 3.0), Tuple3.of(3.0, new Double[] {0.5, 0.7}, 4.0), Tuple3.of
//			(2.0,
//			new Double[] {0.8, 0.9}, 2.0)};
//		for (int i = 0; i < predict.length; i++) {
//			Assert.assertEquals(list.get(i).f0, predict[i].f0, 0.01);
//			Assert.assertEquals(list.get(i).f1.f0, predict[i].f1[0], 0.01);
//			Assert.assertEquals(list.get(i).f1.f1, predict[i].f1[1], 0.01);
//			Assert.assertEquals(list.get(i).f2, predict[i].f2, 0.01);
//		}
//	}

	@Test
	public void isotonicRegTest() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(5);
		int length = 15;
		Object[][] inTrain=new Object[length][3];
		for (int i=0;i<length;++i){
			inTrain[i][0]=tuples[i].f0;
			inTrain[i][1]=tuples[i].f1;
			inTrain[i][2]=tuples[i].f2;
		}
		Object[][] inTest=new Object[10][3];
		for (int i=0;i<10;++i){
			inTest[i][0]=tupleTest[i].f0;
			inTest[i][1]=tupleTest[i].f1;
			inTest[i][2]=tupleTest[i].f2;
		}
		String[] colNames=new String[]{"col1","col2","col3"};
		MemSourceBatchOp trainData=new MemSourceBatchOp(inTrain,colNames);
		MemSourceBatchOp predictData = new MemSourceBatchOp(inTest, new String[]{"col1","col2","col3"});
		IsotonicRegTrainBatchOp model=new IsotonicRegTrainBatchOp().setLabelCol("col1").setFeatureCol("col2").setWeightCol("col3")
				.linkFrom(trainData);
		model.print();
		IsotonicRegPredictBatchOp predictBatchOp = new IsotonicRegPredictBatchOp().setPredictionCol("predictCol");
		predictBatchOp.linkFrom(model,predictData).print();
	}

}