package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocHashCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocHashCountVectorizerPredictStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for DocHashIDFVectorizer.
 */

public class DocHashCountVectorizerTest extends AlinkTestBase {
	private static final Row[] rows = new Row[] {
		Row.of(0, "a b c d a a", 1),
		Row.of(1, "c c b a e", 1)
	};

	private static final List <Row> expected = Arrays.asList(
		Row.of(0, "a b c d a a", 1, VectorUtil.getVector("$10$3:1.0 4:3.0 5:1.0 7:1.0")),
		Row.of(1, "c c b a e", 1, VectorUtil.getVector("$10$4:1.0 5:2.0 6:1.0 7:1.0"))
	);

	@Test
	public void testDefault() throws Exception {
		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "sentence", "label"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "sentence", "label"});

		DocHashCountVectorizer op = new DocHashCountVectorizer()
			.setSelectedCol("sentence")
			.setNumFeatures(10)
			.setOutputCol("res");
		DocHashCountVectorizerModel model = op.fit(data);
		assertListRowEqualWithoutOrder(expected, model.transform(data).collect());

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(model.transform(dataStream));
		StreamOperator.execute();
		assertListRowEqualWithoutOrder(expected, sink.getAndRemoveValues());
	}

	@Test
	public void testInitializer() {
		DocHashCountVectorizerModel model = new DocHashCountVectorizerModel();
		Assert.assertEquals(model.getParams().size(), 0);

		DocHashCountVectorizer op = new DocHashCountVectorizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator <?> b = new DocHashCountVectorizerTrainBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocHashCountVectorizerTrainBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		b = new DocHashCountVectorizerPredictBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new DocHashCountVectorizerPredictBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator <?> s = new DocHashCountVectorizerPredictStreamOp(b);
		Assert.assertEquals(s.getParams().size(), 0);
		s = new DocHashCountVectorizerPredictStreamOp(b, new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
