package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.nlp.Tokenizer;
import org.junit.Test;

/**
 * Example for Tokenizer.
 */
public class TokenizerExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {0, "Hello this is a good book"}),
			Row.of(new Object[] {1, "I am a good boy"})
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});

		Tokenizer tokenizer = new Tokenizer()
			.setSelectedCol("sentence")
			.setOutputCol("token");

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, tokenizer.transform(data)).print();
	}
}
