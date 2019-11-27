package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for NGram.
 */
public class NGramTest {
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

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("sentence"), String.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new String[0]), new String[] {"a_a a_b b_b b_c c_c c_a"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();

	}
}
