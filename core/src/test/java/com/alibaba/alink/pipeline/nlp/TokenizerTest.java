package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.TokenizerBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.TokenizerStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

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

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence"});

		Tokenizer op = new Tokenizer()
			.setSelectedCol("sentence")
			.setOutputCol("token");

		Table res = op.transform(data);

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select
				("token"),
			String.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new String[0]), new String[] {"hello this is a good book"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testInitializer() {
		Tokenizer op = new Tokenizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new TokenizerBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new TokenizerBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new TokenizerStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new TokenizerStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
