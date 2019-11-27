package examples;

import com.alibaba.alink.pipeline.TestUtil;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScalerModel;
import org.junit.Test;

public class StandardScalerExample {

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
}