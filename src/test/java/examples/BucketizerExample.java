package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.feature.Bucketizer;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * Example for Bucketizer.
 */
public class BucketizerExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(-999.9, -999.9),
			Row.of(-0.5, -0.2),
			Row.of(-0.3, -0.1),
			Row.of(0.0, 0.0),
			Row.of(0.2, 0.4),
			Row.of(999.9, 999.9)
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"features1", "features2"});
		String splits = "-Infinity: -0.5: 0.0: 0.5: Infinity";

        Bucketizer op = new Bucketizer()
            .setSelectedCols(new String[]{"features1"})
            .setOutputCols(new String[]{"bucket1"})
            .setSplitsArray(splits);

		Table res = op.transform(data);

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();
	}
}
