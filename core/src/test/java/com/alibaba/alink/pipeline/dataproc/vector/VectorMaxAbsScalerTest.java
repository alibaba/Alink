package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalarModelInfo;
import com.alibaba.alink.pipeline.TestUtil;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.apache.flink.table.api.Table;
import org.junit.Test;

public class VectorMaxAbsScalerTest {

	private static void testPipelineI() throws Exception {

		///////////////// dense vector /////////////
		Table vectorSource = GenerateData.getDenseBatch();
		Table vectorSSource = GenerateData.getDenseStream();

		String selectedColName = "vec";

		VectorMaxAbsScaler scaler = new VectorMaxAbsScaler()
			.setSelectedCol(selectedColName);

		VectorMaxAbsScalerModel denseModel = scaler.fit(vectorSource);
		new TableSourceBatchOp(denseModel.getModelData()).lazyPrint(-1);
		new TableSourceBatchOp(denseModel.transform(vectorSource)).print();
		TestUtil.printTable(denseModel.transform(vectorSSource));

	}

	@Test
	public void testPipeline() throws Exception {
		testPipelineI();
	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch());
		VectorMaxAbsScalerTrainBatchOp trainOp = new VectorMaxAbsScalerTrainBatchOp()
			.setSelectedCol("vec")
			.linkFrom(batchData);
		VectorMaxAbsScalarModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMaxsAbs().length);
		System.out.println(modelInfo.toString());

	}

}