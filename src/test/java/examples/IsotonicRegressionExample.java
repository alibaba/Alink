package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.regression.IsotonicRegression;
import org.junit.Test;

/**
 * Example for IsotonicRegression.
 */
public class IsotonicRegressionExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {0.35, 1}),
			Row.of(new Object[] {0.6, 1}),
			Row.of(new Object[] {0.55, 1}),
			Row.of(new Object[] {0.5, 1}),
			Row.of(new Object[] {0.18, 0}),
			Row.of(new Object[] {0.1, 1}),
			Row.of(new Object[] {0.8, 1}),
			Row.of(new Object[] {0.45, 0}),
			Row.of(new Object[] {0.4, 1}),
			Row.of(new Object[] {0.7, 0}),
			Row.of(new Object[] {0.02, 0}),
			Row.of(new Object[] {0.3, 0}),
			Row.of(new Object[] {0.27, 1}),
			Row.of(new Object[] {0.2, 0}),
			Row.of(new Object[] {0.9, 1})
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"feature", "label"});

		IsotonicRegression op = new IsotonicRegression()
			.setFeatureCol("feature")
			.setLabelCol("label")
			.setPredictionCol("result");

		Pipeline pipeline = new Pipeline().add(op);

		Table res = pipeline.fit(data).transform(data);

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();
	}
}
