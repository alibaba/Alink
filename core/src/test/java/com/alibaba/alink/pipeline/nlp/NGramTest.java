package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.NGramBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.NGramStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for NGram.
 */
public class NGramTest extends AlinkTestBase {
	@Test
	public void testNGram() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "a a b b c c a")
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence"});

		NGram op = new NGram()
			.setSelectedCol("sentence");

		Table res = op.transform(data);

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
			res.select("sentence"), String.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new String[0]), new String[] {"a_a a_b b_b b_c c_c c_a"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();

	}

	@Test
	public void testInitializer() {
		NGram op = new NGram(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new NGramBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new NGramBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new NGramStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new NGramStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
