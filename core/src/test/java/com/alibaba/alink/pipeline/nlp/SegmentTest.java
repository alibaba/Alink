package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for Segment.
 */

public class SegmentTest extends AlinkTestBase {
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

		List <String> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
			res.select("output"), String.class)
			.collect();

		Assert.assertEquals(list.toArray(new String[0]), new String[] {"别人 复习 是 查漏 补缺"});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();

	}

	@Test
	public void testInitializer() {
		Segment op = new Segment(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new SegmentBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new SegmentBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new SegmentStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new SegmentStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
