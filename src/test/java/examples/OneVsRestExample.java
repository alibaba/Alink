package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.OneVsRest;
import com.alibaba.alink.pipeline.classification.OneVsRestModel;
import org.junit.Test;

/**
 * Example for OneVsRest.
 */
public class OneVsRestExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	@Test
	public void main() throws Exception {
		Row[] data = new Row[] {
			Row.of(1.0, 7.0, 9.0, 2),
			Row.of(1.0, 3.0, 3.0, 3),
			Row.of(1.0, 2.0, 4.0, 1),
			Row.of(1.0, 2.0, 4.0, 1)
		};

		Table input = MLEnvironmentFactory.getDefault().createBatchTable(data, new String[] {"f0", "f1", "f2", "label"});

		LogisticRegression lr = new LogisticRegression()
			.setFeatureCols("f0", "f1", "f2")
			.setLabelCol("label")
			.setMaxIter(100);

		OneVsRest oneVsRest = new OneVsRest()
			.setClassifier(lr).setNumClass(3);

		OneVsRestModel model = oneVsRest.fit(input);

		model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail");
		printTable(model.transform(input));
	}
}
