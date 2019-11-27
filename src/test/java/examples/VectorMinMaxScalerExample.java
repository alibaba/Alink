package examples;

import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MinMaxScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MinMaxScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.MinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.MinMaxScalerModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScalerModel;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.junit.Test;

public class VectorMinMaxScalerExample {
	public static void test(boolean hasMax, double max,
							boolean hasMin, double min) throws Exception {

		///////////////// vector /////////////
		BatchOperator vectorSource = new TableSourceBatchOp(GenerateData.getDenseBatch());
		String selectedColName = "vec";

		VectorMinMaxScalerTrainBatchOp vectorStatOp = new VectorMinMaxScalerTrainBatchOp()
			.setSelectedCol(selectedColName);
		if (hasMax) {
			vectorStatOp.setMax(max);
		}
		if (hasMin) {
			vectorStatOp.setMin(min);
		}

		vectorSource.link(vectorStatOp);

		VectorMinMaxScalerPredictBatchOp vectorOp = new VectorMinMaxScalerPredictBatchOp();

		vectorOp.linkFrom(vectorStatOp, vectorSource).print();

		///////////////// sparse vector /////////////
		BatchOperator sparseSource = new TableSourceBatchOp(GenerateData.getSparseBatch());

		VectorMinMaxScalerTrainBatchOp sparseStatOp = new VectorMinMaxScalerTrainBatchOp()
			.setSelectedCol(selectedColName);

		if (hasMax) {
			sparseStatOp.setMax(max);
		}
		if (hasMin) {
			sparseStatOp.setMin(min);
		}

		sparseSource.link(sparseStatOp);

		VectorMinMaxScalerPredictBatchOp sparseOp = new VectorMinMaxScalerPredictBatchOp();

		sparseOp.linkFrom(sparseStatOp, sparseSource).print();

		//////////////// table //////////////
		BatchOperator tableSource = new TableSourceBatchOp(GenerateData.getBatchTable());
		String[] selectColNames = new String[] {"f0", "f1"};

		MinMaxScalerTrainBatchOp tableStatOp = new MinMaxScalerTrainBatchOp()
			.setSelectedCols(selectColNames);

		if (hasMax) {
			tableStatOp.setMax(max);
		}
		if (hasMin) {
			tableStatOp.setMin(min);
		}

		tableSource.link(tableStatOp);

		MinMaxScalerPredictBatchOp tableOp = new MinMaxScalerPredictBatchOp();

		tableOp.linkFrom(tableStatOp, tableSource).print();
	}

	public static void testPipeline(boolean hasMax, double max,
									boolean hasMin, double min) throws Exception {

		///////////////// dense vector /////////////
		Table vectorSource = GenerateData.getDenseBatch();
		Table vectorSSource = GenerateData.getDenseStream();

		String selectedColName = "vec";

		VectorMinMaxScaler scaler = new VectorMinMaxScaler()
			.setSelectedCol(selectedColName);

		if (hasMax) {
			scaler.setMax(max);
		}
		if (hasMin) {
			scaler.setMin(min);
		}

		VectorMinMaxScalerModel denseModel = scaler.fit(vectorSource);
		TestUtil.printTable(denseModel.transform(vectorSource));
		TestUtil.printTable(denseModel.transform(vectorSSource));

		///////////////// sparse vector /////////////
		Table sparseSource = GenerateData.getSparseBatch();
		Table sSparseSource = GenerateData.getSparseStream();

		VectorMinMaxScaler scaler2 = new VectorMinMaxScaler()
			.setSelectedCol(selectedColName);

		if (hasMax) {
			scaler2.setMax(max);
		}
		if (hasMin) {
			scaler2.setMin(min);
		}

		VectorMinMaxScalerModel denseModel2 = scaler2.fit(sparseSource);
		TestUtil.printTable(denseModel2.transform(sparseSource));
		TestUtil.printTable(denseModel2.transform(sSparseSource));

		//////////////// table //////////////
		Table tableSource = GenerateData.getBatchTable();
		Table sTableSource = GenerateData.getStreamTable();

		String[] selectedColNames = new String[] {"f0", "f1"};

		MinMaxScaler scaler3 = new MinMaxScaler()
			.setSelectedCols(selectedColNames);

		if (hasMax) {
			scaler3.setMax(max);
		}
		if (hasMin) {
			scaler3.setMin(min);
		}

		MinMaxScalerModel denseModel3 = scaler3.fit(tableSource);
		TestUtil.printTable(denseModel3.transform(tableSource));
		TestUtil.printTable(denseModel3.transform(sTableSource));
	}

	@Test
	public void test1() throws Exception {
		test(false, 0, false, 0);
	}

	@Test
	public void test2() throws Exception {
		test(true, 2, false, 0);
	}

	@Test
	public void test3() throws Exception {
		test(false, 0, true, -3);
	}

	@Test
	public void test4() throws Exception {
		test(true, 2, false, -3);
	}

	@Test
	public void testPipeline1() throws Exception {
		testPipeline(false, 0, false, 0);
	}

	@Test
	public void testPipeline2() throws Exception {
		testPipeline(true, 2, false, 0);
	}

	@Test
	public void testPipeline3() throws Exception {
		testPipeline(false, 0, true, -3);
	}

	@Test
	public void testPipeline4() throws Exception {
		testPipeline(true, 2, false, -3);
	}

	@Test
	public void test() throws Exception {
		BatchOperator source = new TableSourceBatchOp(GenerateData.getMultiTypeBatchTable());
		String[] selectedColNames = new String[] {"f_long", "f_int", "f_double"};
		MinMaxScaler scaler = new MinMaxScaler()
			.setSelectedCols(selectedColNames)
			.setOutputCols(selectedColNames);

		MinMaxScalerModel model = scaler.fit(source);
		model.transform(source).print();
	}
}
