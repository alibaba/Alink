package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.recommendation.ALS;
import com.alibaba.alink.pipeline.recommendation.ALSModel;
import org.junit.Test;

/**
 * Example for ALS.
 */
public class AlsExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1L, 1L, 0.6),
			Row.of(2L, 2L, 0.8),
			Row.of(2L, 3L, 0.6),
			Row.of(3L, 1L, 0.6),
			Row.of(3L, 2L, 0.3),
			Row.of(3L, 3L, 0.4),
		};

		MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"user", "item", "rating"});

		ALS als = new ALS()
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rating")
			.setLambda(0.001)
			.setRank(10)
			.setNumIter(10);

		ALSModel model = als.fit(data);
		printTable(model.setPredictionCol("predicted_rating").transform(data));
	}
}
