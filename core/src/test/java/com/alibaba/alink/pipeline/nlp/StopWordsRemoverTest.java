package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

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
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence"});

		StopWordsRemover op = new StopWordsRemover()
			.setSelectedCol("sentence")
			.setOutputCol("output");

		Table res = op.transform(data);

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
			res.select("output"), String.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new String[0]), new String[] {"good book"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();

	}

	@Test
	public void testInitializer() {
		StopWordsRemover op = new StopWordsRemover(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new StopWordsRemoverBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new StopWordsRemoverBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new StopWordsRemoverStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new StopWordsRemoverStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
