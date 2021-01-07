package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocHashCountVectorizerPredictStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

//import com.alibaba.alink.common.utils.RowTypeDataStream;

/**
 * Test for DocHashIDFVectorizer.
 */
public class DocHashCountVectorizerTest extends AlinkTestBase {
	private static Row[] rows = new Row[] {
		Row.of(0, "a b c d a a", 1),
		Row.of(1, "c c b a e", 1)
	};

	@Test
	public void testIdf() throws Exception {
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence",
			"label"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows,
			new String[] {"id", "sentence", "label"});

		DocHashCountVectorizer op = new DocHashCountVectorizer()
			.setSelectedCol("sentence")
			.setNumFeatures(10)
			.setOutputCol("res");

		DocHashCountVectorizerModel model = op.fit(data);

		Table res = model.transform(data);

		Assert.assertArrayEquals(
			MLEnvironmentFactory
				.getDefault()
				.getBatchTableEnvironment()
				.toDataSet(res.select("res"), new RowTypeInfo(VectorTypes.SPARSE_VECTOR))
				.collect()
				.stream()
				.map(row -> (SparseVector) row.getField(0))
				.toArray(SparseVector[]::new),
			new SparseVector[] {
				new SparseVector(10, new int[] {3, 4, 5, 7}, new double[] {1.0, 3.0, 1.0, 1.0}),
				new SparseVector(10, new int[] {4, 5, 6, 7}, new double[] {1.0, 2.0, 1.0, 1.0})
			}
		);

		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testException() throws Exception {
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"id", "sentence", "label"});

		DocHashCountVectorizerTrainBatchOp op = new DocHashCountVectorizerTrainBatchOp()
			.setSelectedCol("sentence")
			.setMinDF(2.)
			.setFeatureType("TF")
			.linkFrom(data);

		op.collect();
	}

	@Test
	public void testInitializer() {
		DocHashCountVectorizerModel model = new DocHashCountVectorizerModel();
		Assert.assertEquals(model.getParams().size(), 0);

		DocHashCountVectorizer op = new DocHashCountVectorizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new DocHashCountVectorizerTrainBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocHashCountVectorizerTrainBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		b = new DocHashCountVectorizerPredictBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocHashCountVectorizerPredictBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new DocHashCountVectorizerPredictStreamOp(b);
		Assert.assertEquals(s.getParams().size(), 0);
		s = new DocHashCountVectorizerPredictStreamOp(b, new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
