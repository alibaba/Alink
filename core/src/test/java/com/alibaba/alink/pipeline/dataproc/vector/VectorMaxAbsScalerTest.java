package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.table.api.Table;

import com.alibaba.alink.pipeline.TestUtil;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class VectorMaxAbsScalerTest extends AlinkTestBase {

	private static void testPipelineI() throws Exception {

		///////////////// dense vector /////////////
		Table vectorSource = GenerateData.getDenseBatch();
		Table vectorSSource = GenerateData.getDenseStream();

		String selectedColName = "vec";

		VectorMaxAbsScaler scaler = new VectorMaxAbsScaler()
			.setSelectedCol(selectedColName);

		VectorMaxAbsScalerModel denseModel = scaler.fit(vectorSource);
		TestUtil.printTable(denseModel.getModelData().getOutputTable());

		TestUtil.printTable(denseModel.transform(vectorSource));
		TestUtil.printTable(denseModel.transform(vectorSSource));

	}

	@Test
	public void testPipeline() throws Exception {
		testPipelineI();
	}

}