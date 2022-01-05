package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.junit.Test;

public class BertTextEmbeddingStreamOpTest {
	@Test
	public void linkFrom() throws Exception {
		Row[] rows1 = new Row[] {
			Row.of(1L, "An english sentence."),
			Row.of(2L, "这是一个中文句子"),
		};

		int savedStreamParallelism = MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().getParallelism();
		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().setParallelism(1);

		StreamOperator <?> data = StreamOperator.fromTable(
			MLEnvironmentFactory.getDefault().createStreamTable(rows1, new String[] {"sentence_id", "sentence_text"}));

		BertTextEmbeddingStreamOp bertEmb = new BertTextEmbeddingStreamOp()
			.setSelectedCol("sentence_text").setOutputCol("embedding").setLayer(-2);
		data.link(bertEmb).print();

		StreamOperator.execute();
		StreamOperator.setParallelism(savedStreamParallelism);
	}
}
