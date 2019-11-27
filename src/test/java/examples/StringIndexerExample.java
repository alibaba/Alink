package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import com.alibaba.alink.pipeline.dataproc.StringIndexerModel;
import org.junit.Test;

/**
 * Example for StringIndexer.
 */
public class StringIndexerExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of("apple"),
			Row.of("apple"),
			Row.of("banana"),
			Row.of("banana"),
			Row.of("banana"),
			Row.of("strawberry"),
			Row.of("orange"),
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"tokens"});

		StringIndexer si = new StringIndexer()
			.setSelectedCol("tokens")
			.setStringOrderType("frequency_desc");

		StringIndexerModel model = si.fit(data);
		printTable(model.setOutputCol("index").transform(data));
	}
}
