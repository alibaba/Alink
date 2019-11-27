package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassificationModel;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier;
import org.junit.Test;

/**
 * Example for MultilayerPerceptronClassifier.
 */
public class MultilayerPerceptronClassifierExample {
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

		MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
			.setFeatureCols("f0", "f1", "f2")
			.setLabelCol("label")
			.setLayers(new int[] {3, 3, 3});

		MultilayerPerceptronClassificationModel model = mlpc.fit(input);
		printTable(model.setPredictionCol("pred").transform(input));
	}
}
