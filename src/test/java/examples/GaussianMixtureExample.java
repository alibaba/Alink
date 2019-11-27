package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.clustering.GaussianMixtureModel;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.clustering.GaussianMixture;
import org.junit.Test;

/**
 * Example for GaussianMixture.
 */
public class GaussianMixtureExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of("0 0 0"),
			Row.of("0.1 0.1 0.1"),
			Row.of("0.2 0.2 0.2"),
			Row.of("9 9 9"),
			Row.of("9.1 9.1 9.1"),
			Row.of("9.2 9.2 9.2"),
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"features"});

		GaussianMixture gmm = new GaussianMixture()
			.setVectorCol("features").setPredictionCol("cluster_id").setPredictionDetailCol("detail")
			.setK(2).setMaxIter(100);

		GaussianMixtureModel model = gmm.fit(data);
		BatchOperator.fromTable(model.getModelData()).print();
		printTable(model.transform(data));
	}
}
