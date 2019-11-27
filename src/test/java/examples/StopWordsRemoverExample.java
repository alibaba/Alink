package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.nlp.StopWordsRemover;
import org.junit.Test;

/**
 * Example for StopWordsRemover.
 */
public class StopWordsRemoverExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {0, "This is a good book"}),
			Row.of(new Object[] {1, "这 是 一本 好 书"})
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});

		StopWordsRemover stopWordsRemover = new StopWordsRemover()
			.setSelectedCol("sentence");

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, stopWordsRemover.transform(data)).print();

	}
}
