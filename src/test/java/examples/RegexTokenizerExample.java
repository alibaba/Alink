package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.nlp.RegexTokenizer;
import com.alibaba.alink.pipeline.nlp.StopWordsRemover;
import org.junit.Test;

/**
 * Example for RegexTokenizer.
 */
public class RegexTokenizerExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {0, "That is a English book!", 1}),
			Row.of(new Object[] {1, "That Math book in on the desk.", 1}),
			Row.of(new Object[] {2, "Do you like math?", 1}),
			Row.of(new Object[] {3, "It is a good day today.", 0}),
			Row.of(new Object[] {4, "Have a good day!", 0})
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence", "label"});

		RegexTokenizer tokenizer = new RegexTokenizer()
			.setSelectedCol("sentence")
			.setGaps(false)
			.setMinTokenLength(2)
			.setToLowerCase(true)
			.setPattern("\\w+");

		StopWordsRemover stopWordsRemover = new StopWordsRemover()
			.setSelectedCol("sentence");

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, stopWordsRemover.transform(tokenizer.transform(data))).print();

	}
}
