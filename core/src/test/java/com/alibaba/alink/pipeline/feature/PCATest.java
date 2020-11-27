package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

public class PCATest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		testTable();
		testSparse();
		testDense();

		BatchOperator.execute();
	}

	private void testDense() {
		String[] colNames = new String[] {"id", "vec"};

		Object[][] data = new Object[][] {
			{1, "0.1 0.2 0.3 0.4"},
			{2, "0.2 0.1 0.2 0.6"},
			{3, "0.2 0.3 0.5 0.4"},
			{4, "0.3 0.1 0.3 0.7"},
			{5, "0.4 0.2 0.4 0.4"}
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, colNames);

		PCA pca = new PCA()
			.setK(3)
			.setCalculationType("CORR")
			.setPredictionCol("pred")
			.setReservedCols("id")
			.setVectorCol("vec");

		pca.enableLazyPrintModelInfo();

		PCAModel model = pca.fit(source);
		BatchOperator predict = model.transform(source);

		VectorSummarizerBatchOp summarizerOp = new VectorSummarizerBatchOp()
			.setSelectedCol("pred");

		summarizerOp.linkFrom(predict);

		summarizerOp.lazyCollectVectorSummary(
			new Consumer <BaseVectorSummary>() {
				@Override
				public void accept(BaseVectorSummary summary) {
					Assert.assertEquals(3.4416913763379853E-15, Math.abs(summary.sum().get(0)), 10e-8);
				}
			}
		);

	}

	private void testSparse() {
		String[] colNames = new String[] {"id", "vec"};

		Object[][] data = new Object[][] {
			{1, "0:0.1 1:0.2 2:0.3 3:0.4"},
			{2, "0:0.2 1:0.1 2:0.2 3:0.6"},
			{3, "0:0.2 1:0.3 2:0.5 3:0.4"},
			{4, "0:0.3 1:0.1 2:0.3 3:0.7"},
			{5, "0:0.4 1:0.2 2:0.4 3:0.4"}
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, colNames);

		PCA pca = new PCA()
			.setK(3)
			.setCalculationType("CORR")
			.setPredictionCol("pred")
			.setReservedCols("id")
			.setVectorCol("vec");

		pca.enableLazyPrintModelInfo();

		PCAModel model = pca.fit(source);
		BatchOperator predict = model.transform(source);

		VectorSummarizerBatchOp summarizerOp = new VectorSummarizerBatchOp()
			.setSelectedCol("pred");

		summarizerOp.linkFrom(predict);

		summarizerOp.lazyCollectVectorSummary(new Consumer <BaseVectorSummary>() {
			@Override
			public void accept(BaseVectorSummary summary) {
				Assert.assertEquals(3.4416913763379853E-15, Math.abs(summary.sum().get(0)), 10e-8);
			}
		});

	}

	public void testTable() throws Exception {
		String[] colNames = new String[] {"id", "f0", "f1", "f2", "f3"};

		Object[][] data = new Object[][] {
			{1, 0.1, 0.2, 0.3, 0.4},
			{2, 0.2, 0.1, 0.2, 0.6},
			{3, 0.2, 0.3, 0.5, 0.4},
			{4, 0.3, 0.1, 0.3, 0.7},
			{5, 0.4, 0.2, 0.4, 0.4}
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, colNames);

		PCA pca = new PCA()
			.setK(3)
			.setCalculationType("CORR")
			.setPredictionCol("pred")
			.setReservedCols("id")
			.setSelectedCols("f0", "f1", "f2", "f3");

		pca.enableLazyPrintModelInfo();

		PCAModel model = pca.fit(source);
		BatchOperator predict = model.transform(source);

		VectorSummarizerBatchOp summarizerOp = new VectorSummarizerBatchOp()
			.setSelectedCol("pred");

		summarizerOp.linkFrom(predict);

		summarizerOp.lazyCollectVectorSummary(new Consumer <BaseVectorSummary>() {
			@Override
			public void accept(BaseVectorSummary summary) {
				Assert.assertEquals(3.1086244689504383E-15, Math.abs(summary.sum().get(0)), 10e-8);
			}
		});
	}
}