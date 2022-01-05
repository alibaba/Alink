package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocWordCountBatchOpTest extends AlinkTestBase {
	Row[] rows = new Row[] {
		Row.of(1, "I wish I can grow up quickly"),
		Row.of(2, "present in two or more JARS.")
	};

	List <Row> expected = Arrays.asList(
		Row.of(2, "or", 1L),
		Row.of(2, "present", 1L),
		Row.of(2, "two", 1L),
		Row.of(2, "in", 1L),
		Row.of(1, "wish", 1L),
		Row.of(2, "more", 1L),
		Row.of(2, "JARS.", 1L),
		Row.of(1, "can", 1L),
		Row.of(1, "I", 2L),
		Row.of(1, "up", 1L),
		Row.of(1, "grow", 1L),
		Row.of(1, "quickly", 1L)
	);

	@Test
	public void testDocWordCountBatch() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"id", "sentence"});
		DocWordCountBatchOp op = new DocWordCountBatchOp()
			.setDocIdCol("id")
			.setContentCol("sentence")
			.linkFrom(data);
		assertListRowEqualWithoutOrder(expected, op.collect());
	}
}
