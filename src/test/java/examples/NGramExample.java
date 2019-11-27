package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.nlp.NGram;
import com.alibaba.alink.pipeline.nlp.Word2Vec;
import org.junit.Test;

/**
 * Example for NGram.
 */
public class NGramExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "That is an English book", 1),
			Row.of(1, "That Math book in on the desk", 1),
			Row.of(2, "Do you like math", 1),
			Row.of(3, "It is a good day today", 0),
			Row.of(4, "Have a good day", 0)
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence", "label"});

		NGram nGram = new NGram()
			.setSelectedCol("sentence");

		Word2Vec word2Vec = new Word2Vec()
			.setMinCount(1)
			.setSelectedCol("sentence")
			.setVectorSize(5);

		Pipeline pipeline = new Pipeline().add(nGram).add(word2Vec);

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, pipeline.fit(data).transform(data)).print();

	}
}
