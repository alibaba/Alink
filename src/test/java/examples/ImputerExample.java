package examples;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.pipeline.dataproc.Imputer;
import com.alibaba.alink.pipeline.dataproc.ImputerModel;

import org.apache.flink.table.api.Table;

import com.alibaba.alink.operator.stream.StreamOperator;
import org.junit.Test;

public class ImputerExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	private static void printStreamTable(Table table) throws Exception {
		StreamOperator.fromTable(table).print();
	}

	@Test
	public void testPipelineMean() throws Exception {
		String[] selectedColNames = new String[] {"f_double", "f_long", "f_int"};
		String strategy = "mean";

		testPipeline(selectedColNames, strategy);
	}

	public void testPipeline(String[] selectedColNames, String strategy) throws Exception {
		Table source = GenerateData.getMultiTypeBatchTable();

		Imputer fillMissingValue = new Imputer()
			.setSelectedCols(selectedColNames)
			.setStrategy(strategy);

		ImputerModel model = fillMissingValue.fit(source);

		printTable(model.getModelData());

		printTable(model.transform(source));

		Table ssource = GenerateData.getMultiTypeStreamTable();
		printStreamTable(ssource);
	}
}
