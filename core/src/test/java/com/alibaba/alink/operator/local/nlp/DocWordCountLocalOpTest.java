package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.local.Utils.assertListRowEqualWithoutOrder;

public class DocWordCountLocalOpTest extends TestCase {
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
		DocWordCountLocalOp op = new MemSourceLocalOp(rows, new String[] {"id", "sentence"})
			.link(
				new DocWordCountLocalOp()
					.setDocIdCol("id")
					.setContentCol("sentence")
			)
			.print();
		assertListRowEqualWithoutOrder(expected, op.getOutputTable().getRows());
	}
}