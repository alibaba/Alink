package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.DocWordCountStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DocWordCountBatchOpTest extends AlinkTestBase {
	Row[] rows = new Row[] {
		Row.of(1, "I wish I can grow up quickly"),
		Row.of(2, "present in two or more JARS.")
	};

	@Test
	public void testDocWordCount() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"id", "sentence"});
		DocWordCountBatchOp op = new DocWordCountBatchOp()
			.setDocIdCol("id")
			.setContentCol("sentence")
			.linkFrom(data);

		Assert.assertEquals(op.count(), 12);
	}

	@Test
	public void testDocWordCountStream() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"id", "sentence"});
		MemSourceStreamOp dataStream = new MemSourceStreamOp(Arrays.asList(rows), new String[] {"id", "sentence"});

		DocWordCountStreamOp op = new DocWordCountStreamOp()
			.setDocIdCol("id")
			.setContentCol("sentence");

		op.linkFrom(dataStream).print();

		StreamOperator.execute();
	}

}