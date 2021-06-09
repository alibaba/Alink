package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorStandardScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScalerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class VectorStandardScalerTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch())
			.link(new AppendIdBatchOp().setIdCol("id"));
		StreamOperator streamData = new TableSourceStreamOp(GenerateData.getDenseStream());
		VectorStandardScalerTrainBatchOp op = new VectorStandardScalerTrainBatchOp()
			.setWithMean(true).setWithStd(true)
			.setSelectedCol("vec")
			.linkFrom(batchData);
		BatchOperator res = new VectorStandardScalerPredictBatchOp().setOutputCol("vec_1")
			.linkFrom(op, batchData);

		List <Row> list = res.collect();

		Collections.sort(list, new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				return Long.compare((long) o1.getField(1), (long) o2.getField(1));
			}
		});
		assertDv(VectorUtil.getDenseVector(list.get(1).getField(2)),
			new DenseVector(new double[] {-0.9272, -1.1547}));
		assertDv(VectorUtil.getDenseVector(list.get(0).getField(2)),
			new DenseVector(new double[] {-0.1325, 0.5774}));
		assertDv(VectorUtil.getDenseVector(list.get(2).getField(2)),
			new DenseVector(new double[] {1.0596, 0.5774}));

		new VectorStandardScalerPredictStreamOp(op).setOutputCol("vec_1")
			.linkFrom(streamData).print();

		VectorStandardScalerModel model1 = new VectorStandardScaler().setWithMean(true).setWithStd(false)
			.setSelectedCol("vec").setOutputCol("vec_1").fit(batchData);
		list = model1.transform(batchData).collect();
		Collections.sort(list, new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				return Long.compare((long) o1.getField(1), (long) o2.getField(1));
			}
		});
		assertDv(VectorUtil.getDenseVector(list.get(1).getField(2)),
			new DenseVector(new double[] {-2.3333, -3.3333}));
		assertDv(VectorUtil.getDenseVector(list.get(0).getField(2)),
			new DenseVector(new double[] {-0.3333, 1.6666}));
		assertDv(VectorUtil.getDenseVector(list.get(2).getField(2)),
			new DenseVector(new double[] {2.6666, 1.6666}));
		model1.transform(streamData).print();

		VectorStandardScalerModel model2 = new VectorStandardScaler().setWithMean(false).setWithStd(true)
			.setSelectedCol("vec").setOutputCol("vec_1").fit(batchData);
		list = model2.transform(batchData).collect();
		Collections.sort(list, new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				return Long.compare((long) o1.getField(1), (long) o2.getField(1));
			}
		});

		assertDv(VectorUtil.getDenseVector(list.get(1).getField(2)),
			new DenseVector(new double[] {-0.3974, -1.0392}));
		assertDv(VectorUtil.getDenseVector(list.get(0).getField(2)),
			new DenseVector(new double[] {0.3974, 0.6928}));
		assertDv(VectorUtil.getDenseVector(list.get(2).getField(2)),
			new DenseVector(new double[] {1.5894, 0.6928}));
		model2.transform(streamData).print();

		VectorStandardScalerModel model3 = new VectorStandardScaler().setWithMean(false).setWithStd(false)
			.setSelectedCol("vec").setOutputCol("vec_1").fit(batchData);
		list = model3.transform(batchData).collect();
		Collections.sort(list, new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				return Long.compare((long) o1.getField(1), (long) o2.getField(1));
			}
		});

		assertDv(VectorUtil.getDenseVector(list.get(1).getField(2)),
			new DenseVector(new double[] {-1., -3.}));
		assertDv(VectorUtil.getDenseVector(list.get(0).getField(2)),
			new DenseVector(new double[] {1., 2.}));
		assertDv(VectorUtil.getDenseVector(list.get(2).getField(2)),
			new DenseVector(new double[] {4., 2.}));
		model3.transform(streamData).print();

		StreamOperator.execute();
	}

	public static void assertDv(DenseVector dv1, DenseVector dv2) {
		int length = dv1.size();
		for (int i = 0; i < length; i++) {
			assertEquals((double) dv1.get(i), (double) dv2.get(i), 1e-3);
		}
	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getDenseBatch());
		VectorStandardScalerTrainBatchOp trainOp = new VectorStandardScalerTrainBatchOp()
			.setWithMean(true).setWithStd(true)
			.setSelectedCol("vec")
			.linkFrom(batchData);
		VectorStandardScalerModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMeans().length);
		System.out.println(modelInfo.getStdDevs().length);
		System.out.println(modelInfo.isWithMeans());
		System.out.println(modelInfo.isWithStdDevs());
		System.out.println(modelInfo.toString());

	}
}
