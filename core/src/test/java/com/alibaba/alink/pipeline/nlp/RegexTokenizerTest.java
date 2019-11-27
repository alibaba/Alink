package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for RegexTokenizer.
 */
public class RegexTokenizerTest {
	@Test
	public void testTokenize() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "Hello this is a good book!")
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence"});

		RegexTokenizer op = new RegexTokenizer()
			.setSelectedCol("sentence")
			.setGaps(false)
			.setMinTokenLength(2)
			.setToLowerCase(true)
			.setOutputCol("token")
			.setPattern("\\w+");

		Table res = op.transform(data);

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("token"), String.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new String[0]), new String[] {"hello this is good book"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}
}
