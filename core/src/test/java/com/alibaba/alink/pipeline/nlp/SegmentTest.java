package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
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
		List <Row> expected = Arrays.asList(
			Row.of(1, "别人复习是查漏补缺", "别人 复习 是 查漏 补缺")
		);

		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "sentence"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "sentence"});

		Segment op = new Segment()
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
		Segment op = new Segment(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator <?> b = new SegmentBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new SegmentBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator <?> s = new SegmentStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new SegmentStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
