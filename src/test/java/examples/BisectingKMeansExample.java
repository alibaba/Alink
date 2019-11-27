package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.clustering.BisectingKMeans;
import org.junit.Test;

/**
 * Example for BisectingKMeans.
 */
public class BisectingKMeansExample {
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

		BisectingKMeans bikmeans = new BisectingKMeans()
			.setVectorCol("features").setPredictionCol("cluster_id").setPredictionDetailCol("detail")
			.setK(2).setMaxIter(10);

		printTable(bikmeans.fit(data).transform(data));
	}
}
