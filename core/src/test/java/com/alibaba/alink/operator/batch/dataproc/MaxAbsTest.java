package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalarModelInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.MaxAbsScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.pipeline.dataproc.MaxAbsScaler;
import com.alibaba.alink.pipeline.dataproc.MaxAbsScalerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MaxAbsTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getBatchTable());
		StreamOperator streamData = new TableSourceStreamOp(GenerateData.getStreamTable());
		MaxAbsScalerModel model = new MaxAbsScaler()
			.setSelectedCols("f0", "f1").setOutputCols("f0_1", "f1_1").fit(batchData);
		model.transform(batchData).lazyCollect();
		model.transform(streamData).print();

		MaxAbsScalerTrainBatchOp op = new MaxAbsScalerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.linkFrom(batchData);
		List <Row> rows = new MaxAbsScalerPredictBatchOp().linkFrom(op, batchData).collect();
		rows.sort(StandardScalerTest.compare);
		assertEquals(rows.get(0), Row.of(null, null));
		StandardScalerTest.assertRow(rows.get(1), Row.of(-0.25, -1.));
		StandardScalerTest.assertRow(rows.get(2), Row.of(0.25, 0.666));
		StandardScalerTest.assertRow(rows.get(3), Row.of(1.0, 0.6666));
		new MaxAbsScalerPredictStreamOp(op).linkFrom(streamData).print();
		StreamOperator.execute();
	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getBatchTable());
		MaxAbsScalerTrainBatchOp trainOp = new MaxAbsScalerTrainBatchOp()
			.setSelectedCols("f0")
			.linkFrom(batchData);
		MaxAbsScalarModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMaxAbs().length);
		System.out.println(modelInfo.toString());

	}

}
