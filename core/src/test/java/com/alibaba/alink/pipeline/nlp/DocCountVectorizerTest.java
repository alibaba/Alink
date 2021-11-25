package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocCountVectorizerPredictStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

/**
 * Test for DocCountVectorizer.
 */

public class DocCountVectorizerTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private Row[] rows = new Row[] {
		Row.of(0, "That is an English book", 1),
		Row.of(1, "Have a good day", 1)
	};

	@Test
	public void testDefault() throws Exception {
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence",
			"label"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows,
			new String[] {"id", "sentence", "label"});

		DocCountVectorizer op = new DocCountVectorizer()
			.setSelectedCol("sentence")
			.setOutputCol("features")
			.setFeatureType("TF");

		PipelineModel model = new Pipeline().add(op).fit(data);

		Table res = model.transform(data);

		List <SparseVector> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
			res.select("features"), SparseVector.class).collect();

		Assert.assertEquals(list.size(), 2);
		Assert.assertEquals(list.get(0).getValues().length, 5);
		Assert.assertEquals(list.get(1).getValues().length, 4);

		for (int i = 0; i < list.get(0).getValues().length; i++) {
			Assert.assertEquals(list.get(0).getValues()[i], 0.2, 0.1);
		}
		for (int i = 0; i < list.get(1).getValues().length; i++) {
			Assert.assertEquals(list.get(1).getValues()[i], 0.25, 0.1);
		}
		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testException() throws Exception {
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"id", "sentence", "label"});

		thrown.expect(RuntimeException.class);
		DocCountVectorizerTrainBatchOp op = new DocCountVectorizerTrainBatchOp()
			.setSelectedCol("sentence")
			.setMinDF(2.)
			.setMaxDF(1.)
			.setFeatureType("TF")
			.linkFrom(data);

		op.collect();
	}

	@Test
	public void testInitializer() {
		DocCountVectorizerModel model = new DocCountVectorizerModel();
		Assert.assertEquals(model.getParams().size(), 0);

		DocCountVectorizer op = new DocCountVectorizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new DocCountVectorizerTrainBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocCountVectorizerTrainBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		b = new DocCountVectorizerPredictBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocCountVectorizerPredictBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new DocCountVectorizerPredictStreamOp(b);
		Assert.assertEquals(s.getParams().size(), 0);
		s = new DocCountVectorizerPredictStreamOp(b, new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
