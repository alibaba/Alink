package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalarModelInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorMaxAbsScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

public class VectorMaxAbsTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch());
		StreamOperator streamData = new TableSourceStreamOp(GenerateData.getDenseStream());
		VectorMaxAbsScalerTrainBatchOp op = new VectorMaxAbsScalerTrainBatchOp()
			.setSelectedCol("vec")
			.linkFrom(batchData);

		List <Row> rows = new VectorMaxAbsScalerPredictBatchOp().setOutputCol("res").linkFrom(op, batchData).collect();
		VectorStandardScalerTest.assertDv(VectorUtil.getDenseVector(rows.get(0).getField(1)),
			new DenseVector(new double[] {0.25, 0.6666}));
		VectorStandardScalerTest.assertDv(VectorUtil.getDenseVector(rows.get(1).getField(1)),
			new DenseVector(new double[] {-0.25, -1.}));
		VectorStandardScalerTest.assertDv(VectorUtil.getDenseVector(rows.get(2).getField(1)),
			new DenseVector(new double[] {1., 0.6666}));
		new VectorMaxAbsScalerPredictStreamOp(op).setOutputCol("res").linkFrom(streamData).print();
		StreamOperator.execute();

	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch());
		VectorMaxAbsScalerTrainBatchOp trainOp = new VectorMaxAbsScalerTrainBatchOp()
			.setSelectedCol("vec")
			.linkFrom(batchData);
		VectorMaxAbsScalarModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMaxAbs().length);
		System.out.println(modelInfo.toString());

	}
}
