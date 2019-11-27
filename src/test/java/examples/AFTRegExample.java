package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.regression.AftSurvivalRegression;
import org.junit.Test;

/**
 * Example for AFTRegression.
 */
public class AFTRegExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.218, 15.0, "1.560 -0.605"}),
			Row.of(new Object[] {2.949, 4.0, "0.346 2.158"}),
			Row.of(new Object[] {3.627, 2.0, "1.380 0.231"}),
			Row.of(new Object[] {0.273, 24.0, "0.520 1.151"}),
			Row.of(new Object[] {4.199, 1.0, "0.795 -0.226"})
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"label", "censor", "features"});

		Binarizer binarizer = new Binarizer()
			.setThreshold(8.0)
			.setSelectedCol("censor");

		AftSurvivalRegression reg = new AftSurvivalRegression()
			.setVectorCol("features")
			.setLabelCol("label")
			.setCensorCol("censor")
			.setPredictionCol("result");

		Pipeline pipeline = new Pipeline().add(binarizer).add(reg);

		Table res = pipeline.fit(data).transform(data);

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();
	}
}
