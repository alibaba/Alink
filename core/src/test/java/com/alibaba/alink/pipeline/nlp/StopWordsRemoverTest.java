package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for StopWordsRemover.
 */

public class StopWordsRemoverTest extends AlinkTestBase {
	@Test
	public void testFilterStopWords() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "This is a good book")
		};
		List <Row> expected = Arrays.asList(
			Row.of(0, "This is a good book", "good book")
		);

		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "sentence"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "sentence"});

		StopWordsRemover op = new StopWordsRemover()
			.setSelectedCol("sentence")
			.setOutputCol("output");
		assertListRowEqualWithoutOrder(expected, op.transform(data).collect());

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op.transform(dataStream));
		StreamOperator.execute();
		assertListRowEqualWithoutOrder(expected, sink.getAndRemoveValues());
	}

	@Test
	public void testInitializer() {
		StopWordsRemover op = new StopWordsRemover(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator <?> b = new StopWordsRemoverBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new StopWordsRemoverBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator <?> s = new StopWordsRemoverStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new StopWordsRemoverStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
