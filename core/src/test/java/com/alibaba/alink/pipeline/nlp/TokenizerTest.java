package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.TokenizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.TokenizerStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for Tokenizer.
 */

public class TokenizerTest extends AlinkTestBase {
	@Test
	public void testTokenize() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "Hello this is a good book")
		};
		List <Row> expected = Arrays.asList(
			Row.of(0, "Hello this is a good book", "hello this is a good book")
		);

		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "sentence"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "sentence"});

		Tokenizer op = new Tokenizer()
			.setSelectedCol("sentence")
			.setOutputCol("token");
		assertListRowEqualWithoutOrder(expected, op.transform(data).collect());

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op.transform(dataStream));
		StreamOperator.execute();
		assertListRowEqualWithoutOrder(expected, sink.getAndRemoveValues());
	}

	@Test
	public void testInitializer() {
		Tokenizer op = new Tokenizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator <?> b = new TokenizerBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new TokenizerBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator <?> s = new TokenizerStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new TokenizerStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
