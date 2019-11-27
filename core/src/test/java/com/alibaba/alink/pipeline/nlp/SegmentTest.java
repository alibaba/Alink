package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for Segment.
 */
public class SegmentTest {
	@Test
	public void testSegment() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, "别人复习是查漏补缺")
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence"});

		Segment op = new Segment()
			.setSelectedCol("sentence")
			.setOutputCol("output");

		Table res = op.transform(data);

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("output"), String.class)
			.collect();

		Assert.assertEquals(list.toArray(new String[0]), new String[] {"别人 复习 是 查漏 补缺"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();

	}
}
