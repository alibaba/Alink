package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for StopWordsRemover.
 */
public class StopWordsRemoverTest {
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

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("output"), String.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new String[0]), new String[] {"good book"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();

	}
}
