package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.MinMaxScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.pipeline.dataproc.MinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.MinMaxScalerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MinMaxTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getBatchTable());
		StreamOperator streamData = new TableSourceStreamOp(GenerateData.getStreamTable());

		MinMaxScalerTrainBatchOp op = new MinMaxScalerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.linkFrom(batchData);
		new MinMaxScalerPredictBatchOp().linkFrom(op, batchData).lazyCollect();
		new MinMaxScalerPredictStreamOp(op).linkFrom(streamData).print();

		MinMaxScalerModel model = new MinMaxScaler()
			.setSelectedCols("f0", "f1").setOutputCols("f0_1", "f1_1").fit(batchData);
		List <Row> rows = model.transform(batchData).collect();
		rows.sort(new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				if (o1.getField(0) == null) {
					return -1;
				}
				if (o2.getField(0) == null) {
					return 1;
				}
				if ((double) o1.getField(0) > (double) o2.getField(0)) {
					return 1;
				}
				if ((double) o1.getField(0) < (double) o2.getField(0)) {
					return -1;
				}
				return 0;
			}
		});
		assertEquals(rows.get(0), Row.of(null, null, null, null));
		assertEquals(rows.get(1), Row.of(-1., -3., 0., 0.));
		assertEquals(rows.get(2), Row.of(1., 2., 0.4, 1.));
		assertEquals(rows.get(3), Row.of(4., 2., 1., 1.));
		model.transform(streamData).print();
		StreamOperator.execute();
	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getBatchTable());
		MinMaxScalerTrainBatchOp trainOp = new MinMaxScalerTrainBatchOp()
			.setSelectedCols("f0")
			.linkFrom(batchData);
		MinMaxScalerModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMaxs().length);
		System.out.println(modelInfo.getMins().length);
		System.out.println(modelInfo.toString());
	}
}
