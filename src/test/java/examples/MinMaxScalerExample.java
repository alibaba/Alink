package examples;

import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.pipeline.dataproc.MinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.MinMaxScalerModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScalerModel;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.junit.Test;

public class MinMaxScalerExample {

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
}
