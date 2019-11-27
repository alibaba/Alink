package examples;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.regression.GeneralizedLinearRegression;
import com.alibaba.alink.pipeline.regression.GeneralizedLinearRegressionModel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GeneralizedLinearRegressionExample {
	private static void printTable(Table table) throws Exception {
		BatchOperator.fromTable(table).print();
	}

	@Test
	public void testGamma() throws Exception {

		double[][] data = new double[][] {{1, 5, 118, 69},
			{2, 10, 58, 35},
			{3, 15, 42, 26},
			{4, 20, 35, 21},
			{5, 30, 27, 18},
			{6, 40, 25, 16},
			{7, 60, 21, 13},
			{8, 80, 19, 12},
			{9, 100, 18, 12}
		};

		List <Row> dataRow = new ArrayList <>();
		for (int i = 0; i < data.length; i++) {
			dataRow.add(Row.of(Math.log(data[i][1]), data[i][2], data[i][3], 1.0, 2.0));
		}

		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
			Types.DOUBLE};

		Table source = MLEnvironmentFactory.getDefault().createBatchTable(dataRow,
			colNames);

		String[] featureColNames = new String[] {"lot1", "lot2"};
		String labelColName = "u";

		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFamily("gamma")
			.setLink("Log")
			.setRegParam(0.3)
			//                .setFitIntercept(false)
			.setMaxIter(10)
			//                .setOffsetColName(getOffsetColName())
			//                .setWeightCol(getWeightsColName())
			//                .setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("pred");

		GeneralizedLinearRegressionModel model = glm.fit(source);
		printTable(model.transform(source));
	}

	@Test
	public void testBinomial() throws Exception {
		double[][] data = new double[][] {
			{1, 1, 1, 18},
			{2, 1, 2, 17},
			{3, 1, 3, 15},
			{4, 2, 1, 20},
			{5, 2, 2, 10},
			{6, 2, 3, 20},
			{7, 3, 1, 25},
			{8, 3, 2, 13},
			{9, 3, 3, 12}
		};

		List <Row> dataRow = new ArrayList <>();
		for (int i = 0; i < data.length; i++) {
			dataRow.add(Row.of(data[i][1], data[i][2], data[i][3] % 2, 1.0, 2.0));
		}

		String[] colNames = new String[] {"treatment", "outcome", "counts", "offset", "weights"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
			Types.DOUBLE};

		Table source = MLEnvironmentFactory.getDefault().createBatchTable(dataRow,
			colNames);

		String[] featureColNames = new String[] {"treatment", "outcome"};
		String labelColName = "counts";

		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFamily("Binomial")
			.setLink("Logit")
			//                .setRegParam(0.3)
			//                .setFitIntercept(false)
			.setMaxIter(10)
			//                .setOffsetColName(getOffsetColName())
			//                .setWeightCol(getWeightsColName())
			//                .setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("pred");

		GeneralizedLinearRegressionModel model = glm.fit(source);
		printTable(model.transform(source));
	}

	@Test
	public void testTweedie() throws Exception {
		BatchOperator.setParallelism(2);

		double[][] data = new double[][] {{1, 5, 118, 69},
			{2, 10, 58, 35},
			{3, 15, 42, 26},
			{4, 20, 35, 21},
			{5, 30, 27, 18},
			{6, 40, 25, 16},
			{7, 60, 21, 13},
			{8, 80, 19, 12},
			{9, 100, 18, 12}
		};

		List <Row> dataRow = new ArrayList <>();
		for (int i = 0; i < data.length; i++) {
			dataRow.add(Row.of(Math.log(data[i][1]), data[i][2], data[i][3], 1.0, 2.0));
		}

		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
			Types.DOUBLE};

		Table source = MLEnvironmentFactory.getDefault().createBatchTable(dataRow, colNames);

		String[] featureColNames = new String[] {"lot1", "lot2"};
		String labelColName = "u";

		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFamily("tweedie")
			.setVariancePower(1.65)
			.setLink("power")
			.setLinkPower(-0.5)
			//                .setRegParam(0.3)
			//                .setFitIntercept(false)
			.setMaxIter(10)
			//                .setOffsetColName(getOffsetColName())
			//                .setWeightCol(getWeightsColName())
			//                .setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("pred");

		GeneralizedLinearRegressionModel model = glm.fit(source);
		printTable(model.transform(source));
	}

	@Test
	public void testPossion() throws Exception {
		BatchOperator.setParallelism(2);

		double[][] data = new double[][] {
			{1, 1, 1, 18},
			{2, 1, 2, 17},
			{3, 1, 3, 15},
			{4, 2, 1, 20},
			{5, 2, 2, 10},
			{6, 2, 3, 20},
			{7, 3, 1, 25},
			{8, 3, 2, 13},
			{9, 3, 3, 12}
		};

		List <Row> dataRow = new ArrayList <>();
		for (int i = 0; i < data.length; i++) {
			dataRow.add(Row.of(data[i][1], data[i][2], data[i][3], 1.0, 2.0));
		}

		String[] colNames = new String[] {"treatment", "outcome", "counts", "offset", "weights"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
			Types.DOUBLE};

		Table source = MLEnvironmentFactory.getDefault().createBatchTable(dataRow, colNames);

		String[] featureColNames = new String[] {"treatment", "outcome"};
		String labelColName = "counts";

		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFamily("poisson")
			.setLink("sqrt")
			//                .setRegParam(0.3)
			//                .setFitIntercept(false)
			.setMaxIter(10)
			//                .setOffsetColName(getOffsetColName())
			//                .setWeightCol(getWeightsColName())
			//                .setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("pred");

		GeneralizedLinearRegressionModel model = glm.fit(source);
		printTable(model.transform(source));
	}

	public String[] getFeatureColNames(int colSize) {
		String[] colNames = new String[colSize];

		for (int i = 0; i < colSize; i++) {
			colNames[i] = "feat_" + i;
		}

		return colNames;
	}

	public String getLabelColName() {
		return "label";
	}

	public String getOffsetColName() {
		return "offset";
	}

	public String getWeightsColName() {
		return "weights";
	}

}