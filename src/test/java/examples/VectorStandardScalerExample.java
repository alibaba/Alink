package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StandardScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.StandardScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.dataproc.MinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.MinMaxScalerModel;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.StandardScalerModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScalerModel;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.junit.Test;

import java.util.Arrays;

public class VectorStandardScalerExample {
	public static void test(boolean withMean, boolean withStdv) throws Exception {
		///////////////// vector /////////////
		BatchOperator vectorSource = new TableSourceBatchOp(GenerateData.getDenseBatch());
		String selectedColName = "vec";

		VectorStandardScalerTrainBatchOp vectorStatOp = new VectorStandardScalerTrainBatchOp()
			.setSelectedCol(selectedColName)
			.setWithMean(withMean)
			.setWithStd(withStdv);

		vectorSource.link(vectorStatOp);

		VectorStandardScalerPredictBatchOp vectorOp = new VectorStandardScalerPredictBatchOp();

		vectorOp.linkFrom(vectorStatOp, vectorSource).print();

		/////////////// sparse vector /////////////
		BatchOperator sparseSource = new TableSourceBatchOp(GenerateData.getSparseBatch());

		VectorStandardScalerTrainBatchOp sparseStatOp = new VectorStandardScalerTrainBatchOp()
			.setSelectedCol(selectedColName)
			.setWithMean(withMean)
			.setWithStd(withStdv);

		sparseSource.link(sparseStatOp);

		VectorStandardScalerPredictBatchOp sparseOp = new VectorStandardScalerPredictBatchOp();

		sparseOp.linkFrom(sparseStatOp, sparseSource).print();

		//////////////// table //////////////

		BatchOperator tableSource = new TableSourceBatchOp(GenerateData.getBatchTable());
		String[] selectColNames = new String[] {"f0", "f1"};

		StandardScalerTrainBatchOp tableStatOp = new StandardScalerTrainBatchOp()
			.setSelectedCols(selectColNames)
			.setWithMean(withMean)
			.setWithStd(withStdv);

		tableSource.link(tableStatOp);

		StandardScalerPredictBatchOp tableOp = new StandardScalerPredictBatchOp();

		tableOp.linkFrom(tableStatOp, tableSource).print();
	}

	public static void testPipeline(boolean withMean, boolean withStdv) throws Exception {

		///////////////// dense vector /////////////
		Table vectorSource = GenerateData.getDenseBatch();
		Table vectorSSource = GenerateData.getDenseStream();

		String selectedColName = "vec";

		VectorStandardScaler scaler = new VectorStandardScaler()
			.setSelectedCol(selectedColName)
			.setWithMean(withMean)
			.setWithStd(withStdv);

		VectorStandardScalerModel denseModel = scaler.fit(vectorSource);
		TestUtil.printTable(denseModel.transform(vectorSource));
		TestUtil.printTable(denseModel.transform(vectorSSource));

		///////////////// sparse vector /////////////
		Table sparseSource = GenerateData.getSparseBatch();
		Table sSparseSource = GenerateData.getSparseStream();

		VectorStandardScaler scaler2 = new VectorStandardScaler()
			.setSelectedCol(selectedColName)
			.setWithMean(withMean)
			.setWithStd(withStdv);

		VectorStandardScalerModel denseModel2 = scaler2.fit(sparseSource);
		TestUtil.printTable(denseModel2.transform(sparseSource));
		TestUtil.printTable(denseModel2.transform(sSparseSource));

		//////////////// table //////////////
		Table tableSource = GenerateData.getBatchTable();
		Table sTableSource = GenerateData.getStreamTable();

		String[] selectedColNames = new String[] {"f0", "f1"};

		StandardScaler scaler3 = new StandardScaler()
			.setSelectedCols(selectedColNames)
			.setWithMean(withMean)
			.setWithStd(withStdv);

		StandardScalerModel denseModel3 = scaler3.fit(tableSource);
		TestUtil.printTable(denseModel3.transform(tableSource));
		TestUtil.printTable(denseModel3.transform(sTableSource));
	}

	public static Table getMultiTypeBatchTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1L, 1, 0.2, true}),
				Row.of(new Object[] {null, 2L, 2, null, true}),
				Row.of(new Object[] {"c", null, null, null, false}),
				Row.of(new Object[] {"a", 0L, 0, null, null}),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.STRING, Types.LONG, Types.INT, Types.DOUBLE,
			Types.BOOLEAN};

		DataSet <Row> dataSet = MLEnvironmentFactory.getDefault().getExecutionEnvironment().fromCollection(Arrays.asList(testArray));
		return DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, dataSet, colNames, colTypes);
	}

	@Test
	public void test1() throws Exception {
		test(true, true);
	}

	@Test
	public void test2() throws Exception {
		test(true, false);
	}

	@Test
	public void test3() throws Exception {
		test(false, true);
	}

	@Test
	public void test4() throws Exception {
		test(false, false);
	}

	@Test
	public void testPipeline1() throws Exception {
		testPipeline(true, true);
	}

	@Test
	public void testPipeline2() throws Exception {
		testPipeline(true, false);
	}

	@Test
	public void testPipeline3() throws Exception {
		testPipeline(false, true);
	}

	@Test
	public void testPipeline4() throws Exception {
		testPipeline(false, false);
	}

	@Test
	public void test() throws Exception {
		BatchOperator source = new TableSourceBatchOp(getMultiTypeBatchTable());
		String[] selectedColNames = new String[] {"f_double"};

		MinMaxScaler minMaxScaler = new MinMaxScaler()
			.setSelectedCols(selectedColNames);

		MinMaxScalerModel minMaxScalerModel = minMaxScaler.fit(source);
		BatchOperator scalers = minMaxScalerModel.transform(source);

		StandardScaler scaler = new StandardScaler()
			.setSelectedCols(selectedColNames)
			.setWithMean(true)
			.setWithStd(true);

		StandardScalerModel model = scaler.fit(scalers);
		model.transform(scalers).print();

	}
}