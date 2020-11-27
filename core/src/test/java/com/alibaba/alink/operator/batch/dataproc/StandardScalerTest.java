package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.StandardScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.StandardScalerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class StandardScalerTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getBatchTable());
		StreamOperator streamData = new TableSourceStreamOp(GenerateData.getStreamTable());

		StandardScalerTrainBatchOp op = new StandardScalerTrainBatchOp()
			.setWithMean(true).setWithStd(true)
			.setSelectedCols("f0", "f1")
			.linkFrom(batchData);
		new StandardScalerPredictBatchOp().setOutputCols("f0_1", "f1_1")
			.linkFrom(op, batchData)
			.lazyCollect(new Consumer <List <Row>>() {
				@Override
				public void accept(List <Row> rows) {
					rows.sort(compare);
					assertEquals(rows.get(0), Row.of(null, null, null, null));
					assertRow(rows.get(1), Row.of(-1., -3., -0.9272, -1.1547));
					assertRow(rows.get(2), Row.of(1., 2., -0.1325, 0.5774));
					assertRow(rows.get(3), Row.of(4., 2., 1.0596, 0.5774));
				}
			});
		new StandardScalerPredictStreamOp(op).setOutputCols("f0_1", "f1_1")
			.linkFrom(streamData).print();

		StandardScalerModel model1 = new StandardScaler().setWithMean(true).setWithStd(false)
			.setSelectedCols("f0", "f1").setOutputCols("f0_1", "f1_1").fit(batchData);
		model1.transform(batchData)
			.lazyCollect(new Consumer <List <Row>>() {
				@Override
				public void accept(List <Row> rows) {
					rows.sort(compare);
					assertEquals(rows.get(0), Row.of(null, null, null, null));
					assertRow(rows.get(1), Row.of(-1., -3., -2.3333, -3.3333));
					assertRow(rows.get(2), Row.of(1., 2., -0.3333, 1.6666));
					assertRow(rows.get(3), Row.of(4., 2., 2.6666, 1.6666));
				}
			});
		model1.transform(streamData).print();

		StandardScalerModel model2 = new StandardScaler().setWithMean(false).setWithStd(true)
			.setSelectedCols("f0", "f1").setOutputCols("f0_1", "f1_1").fit(batchData);
		model2.transform(batchData).lazyCollect(new Consumer <List <Row>>() {
			@Override
			public void accept(List <Row> rows) {
				rows.sort(compare);
				assertEquals(rows.get(0), Row.of(null, null, null, null));
				assertRow(rows.get(1), Row.of(-1., -3., -0.3974, -1.0392));
				assertRow(rows.get(2), Row.of(1., 2., 0.3974, 0.6928));
				assertRow(rows.get(3), Row.of(4., 2., 1.5894, 0.6928));
			}
		});
		model2.transform(streamData).print();

		StandardScalerModel model3 = new StandardScaler().setWithMean(false).setWithStd(false)
			.setSelectedCols("f0", "f1").setOutputCols("f0_1", "f1_1").fit(batchData);
		model3.transform(batchData).lazyCollect(new Consumer <List <Row>>() {
			@Override
			public void accept(List <Row> rows) {
				rows.sort(compare);
				assertEquals(rows.get(0), Row.of(null, null, null, null));
				assertRow(rows.get(1), Row.of(-1., -3., -1., -3.));
				assertRow(rows.get(2), Row.of(1., 2., 1., 2.));
				assertRow(rows.get(3), Row.of(4., 2., 4., 2.));
			}
		});
		model3.transform(streamData).print();

		StreamOperator.execute();
	}

	public static Comparator <Row> compare = new Comparator <Row>() {
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
	};

	public static void assertRow(Row dv1, Row dv2) {
		int length = dv1.getArity();
		for (int i = 0; i < length; i++) {
			assertEquals((double) dv1.getField(i), (double) dv2.getField(i), 1e-3);
		}
	}

	@Test
	public void testModelInfo() {
		BatchOperator batchData = new TableSourceBatchOp(GenerateData.getBatchTable());
		StandardScalerTrainBatchOp trainOp = new StandardScalerTrainBatchOp()
			.setWithMean(true).setWithStd(true)
			.setSelectedCols("f0")
			.linkFrom(batchData);
		StandardScalerModelInfo modelInfo = trainOp.getModelInfoBatchOp().collectModelInfo();
		System.out.println(modelInfo.getMeans().length);
		System.out.println(modelInfo.getStdDevs().length);
		System.out.println(modelInfo.isWithMeans());
		System.out.println(modelInfo.isWithStdDevs());
		System.out.println(modelInfo.toString());

	}
}
