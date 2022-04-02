package com.alibaba.alink.operator.batch.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class OcsvmBatchOpTest extends AlinkTestBase {
	@Test
	public void testOutlier() throws Exception {

		BatchOperator <?> data = new MemSourceBatchOp(
			new Object[][] {
				{0.730967787376657, 0.24053641567148587, 0.6374174253501083, 0.5504370051176339},
				{0.7308781907032909, 0.41008081149220166, 0.20771484130971707, 0.3327170559595112},
				{0.7311469360199058, 0.9014476240300544, 0.49682259343089075, 0.9858769332362016},
				{0.731057369148862, 0.07099203475193139, 0.06712000939049956, 0.768156984078079},
				{0.7306094602878371, 0.9187140138555101, 0.9186071189908658, 0.6795571637816596},
				{0.730519863614471, 0.08825840967622589, 0.4889045498516358, 0.461837214623537},
				{0.7307886238322471, 0.5796252073129174, 0.7780122870716483, 0.11499709190022733},
				{0.7306990420600421, 0.7491696031336331, 0.34830970303125697, 0.8972771427421047}
			},
			new String[] {"x1", "x2", "x3", "x4"});

		new OcsvmOutlierBatchOp()
			.setFeatureCols("x1", "x2", "x3", "x4")
			.setGamma(0.5)
			.setNu(0.2)
			.setKernelType("RBF")
			.setPredictionCol("pred").linkFrom(data).print();
	}
}