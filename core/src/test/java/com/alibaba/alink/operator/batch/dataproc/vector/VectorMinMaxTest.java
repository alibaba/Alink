package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorMinMaxScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class VectorMinMaxTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch());
		StreamOperator streamData = new TableSourceStreamOp(GenerateData.getDenseStream());

		VectorMinMaxScalerTrainBatchOp op = new VectorMinMaxScalerTrainBatchOp()
			.setSelectedCol("vec")
			.linkFrom(batchData);
		List <Row> rows = new VectorMinMaxScalerPredictBatchOp().linkFrom(op, batchData).collect();
		assertEquals(rows.get(0).getField(0), new DenseVector(new double[] {0.4, 1.0}));
		assertEquals(rows.get(1), Row.of(new DenseVector(new double[] {0., 0.0})));
		assertEquals(rows.get(2), Row.of(new DenseVector(new double[] {1.0, 1.0})));
		new VectorMinMaxScalerPredictStreamOp(op).linkFrom(streamData).print();
		StreamOperator.execute();
	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch());
		VectorMinMaxScalerTrainBatchOp trainOp = new VectorMinMaxScalerTrainBatchOp()
			.setSelectedCol("vec")
			.linkFrom(batchData);
		VectorMinMaxScalerModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMaxs().length);
		System.out.println(modelInfo.getMins().length);
		System.out.println(modelInfo.toString());

	}
}
