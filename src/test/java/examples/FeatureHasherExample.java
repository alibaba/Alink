package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.feature.FeatureHasher;
import org.junit.Test;

/**
 * Example for FeatureHasher.
 */
public class FeatureHasherExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.1, true, "2", "A"}),
			Row.of(new Object[] {1.1, false, "2", "B"}),
			Row.of(new Object[] {1.1, true, "1", "B"}),
			Row.of(new Object[] {2.2, true, "1", "A"}),
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"double", "bool", "number", "str"});

		FeatureHasher op = new FeatureHasher()
			.setSelectedCols(new String[] {"double", "bool", "number", "str"})
			.setNumFeatures(100)
			.setOutputCol("features");

		Table res = op.transform(data);
		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();
	}
}
