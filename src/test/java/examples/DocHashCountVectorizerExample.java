package examples;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.NaiveBayes;
import com.alibaba.alink.pipeline.nlp.DocHashCountVectorizer;
import com.alibaba.alink.pipeline.nlp.StopWordsRemover;
import org.junit.Test;

/**
 * Example for DocHashIDFVectorizer.
 */
public class DocHashCountVectorizerExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "That is an English book", 1),
			Row.of(1, "That Math book in on the desk", 1),
			Row.of(2, "Do you like math", 1),
			Row.of(3, "It is a good day today", 0),
			Row.of(4, "Have a good day", 0)
		};
		MemSourceBatchOp data = new MemSourceBatchOp(rows, new String[] {"id", "sentence", "label"});

		StopWordsRemover stopWordsRemover = new StopWordsRemover()
			.setSelectedCol("sentence");

		DocHashCountVectorizer docHashIDFVectorizer = new DocHashCountVectorizer()
			.setSelectedCol("sentence")
			.setOutputCol("res");

		Pipeline pipeline = new Pipeline().add(stopWordsRemover).add(docHashIDFVectorizer);

		pipeline.fit(data).transform(data).print();
	}
}
