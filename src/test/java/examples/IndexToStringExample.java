package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.dataproc.IndexToString;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import org.junit.Test;

/**
 * Example for IndexToString.
 */
public class IndexToStringExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	@Test
	public void main() throws Exception {
		Row[] trainRows = new Row[] {
			Row.of("apple"),
			Row.of("apple"),
			Row.of("banana"),
			Row.of("banana"),
			Row.of("banana"),
			Row.of("strawberry"),
			Row.of("orange"),
		};

		Row[] predRows = new Row[] {
			Row.of(0L),
			Row.of(1L),
			Row.of(2L),
			Row.of(3L),
		};

		Table trainData = MLEnvironmentFactory.getDefault().createBatchTable(trainRows, new String[] {"token"});
		Table predData = MLEnvironmentFactory.getDefault().createBatchTable(predRows, new String[] {"index"});

		StringIndexer si = new StringIndexer()
			.setSelectedCol("token")
			.setStringOrderType("frequency_desc")
			.setModelName("si_model");

		si.fit(trainData);

		IndexToString indexToString = new IndexToString()
			.setModelName("si_model")
			.setSelectedCol("index")
			.setOutputCol("token");

		printTable(indexToString.transform(predData));
	}
}
