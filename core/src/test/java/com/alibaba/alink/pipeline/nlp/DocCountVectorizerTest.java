package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocCountVectorizerPredictStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for DocCountVectorizer.
 */

public class DocCountVectorizerTest extends AlinkTestBase {
	private final Row[] rows = new Row[] {
		Row.of(0, "That is an English book", 1),
		Row.of(1, "Have a good day", 1)
	};

	private final List <Row> expected = Arrays.asList(
		Row.of(0, "That is an English book", 1, VectorUtil.getVector("$9$0:0.2 3:0.2 4:0.2 6:0.2 8:0.2")),
		Row.of(1, "Have a good day", 1, VectorUtil.getVector("$9$1:0.25 2:0.25 5:0.25 7:0.25"))
	);

	@Test
	public void testDefault() throws Exception {
		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "sentence", "label"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "sentence", "label"});

		DocCountVectorizer op = new DocCountVectorizer()
			.setSelectedCol("sentence")
			.setOutputCol("features")
			.setFeatureType("TF");
		DocCountVectorizerModel model = op.fit(data);
		assertListRowEqual(expected, model.transform(data).collect(), 0);

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(model.transform(dataStream));
		StreamOperator.execute();
		assertListRowEqual(expected, sink.getAndRemoveValues(), 0);
	}

	@Test(expected = RuntimeException.class)
	public void testException() throws Exception {
		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "sentence", "label"});
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

		BatchOperator <?> b = new DocCountVectorizerTrainBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocCountVectorizerTrainBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		b = new DocCountVectorizerPredictBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocCountVectorizerPredictBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator <?> s = new DocCountVectorizerPredictStreamOp(b);
		Assert.assertEquals(s.getParams().size(), 0);
		s = new DocCountVectorizerPredictStreamOp(b, new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
