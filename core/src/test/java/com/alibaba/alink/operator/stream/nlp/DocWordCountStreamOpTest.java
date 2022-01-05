package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocWordCountStreamOpTest extends AlinkTestBase {
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
	public void testDocWordCountStream() throws Exception {
		MemSourceStreamOp dataStream = new MemSourceStreamOp(Arrays.asList(rows), new String[] {"id", "sentence"});

		DocWordCountStreamOp op = new DocWordCountStreamOp()
			.setDocIdCol("id")
			.setContentCol("sentence")
			.linkFrom(dataStream);
		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op);
		StreamOperator.execute();
		assertListRowEqualWithoutOrder(expected, sink.getAndRemoveValues());
	}
}
