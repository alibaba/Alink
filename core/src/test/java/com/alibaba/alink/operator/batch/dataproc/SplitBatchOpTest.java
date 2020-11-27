package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionBatchOp;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import com.alibaba.alink.pipeline.feature.OneHotEncoderModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SplitBatchOpTest extends AlinkTestBase {
	@Test
	public void split() throws Exception {
		BatchOperator data = Iris.getBatchData();
		data = new AppendIdBatchOp().linkFrom(data);
		BatchOperator spliter = new SplitBatchOp().setFraction(0.1);
		BatchOperator left = spliter.linkFrom(data);
		BatchOperator right = spliter.getSideOutput(0);
		Assert.assertEquals(left.count(), 15);
		Assert.assertEquals(right.count(), 150 - 15);
		Assert.assertEquals(new UnionBatchOp().linkFrom(left, right).count(), 150);
	}

	@Test
	public void testSplitAfterOneHot() throws Exception {
		BatchOperator data = Iris.getBatchData();
		OneHotEncoderModel model = new OneHotEncoder().setSelectedCols(Iris.getFeatureColNames())
			.setReservedCols(Iris.getLabelColName())
			.setOutputCols("features").fit(data);
		data = model.transform(data);

		SplitBatchOp split = new SplitBatchOp().setFraction(0.4);

		BatchOperator data1 = split.linkFrom(data);
		BatchOperator data2 = split.getSideOutput(0);
		Assert.assertEquals(data1.count(), 60);
		Assert.assertEquals(data2.count(), 90);
	}

	@Test
	public void splitWithSeed() throws Exception {
		BatchOperator data = Iris.getBatchData();
		BatchOperator spliter = new SplitBatchOp().setFraction(0.01).setRandomSeed(1);
		BatchOperator left = spliter.linkFrom(data);
		BatchOperator right = spliter.getSideOutput(0);
		List <Row> list = left.collect();
		Assert.assertEquals(list.size(), 2);
		Assert.assertEquals((double) list.get(0).getField(0), 4.8, 0.01);
		Assert.assertEquals((double) list.get(1).getField(0), 6.4, 0.01);
		Assert.assertEquals(right.count(), 148);
		Assert.assertEquals(new UnionBatchOp().linkFrom(left, right).count(), 147);
	}

	@Test
	public void splitWithSeedLargeFraction() throws Exception {
		BatchOperator data = Iris.getBatchData();
		BatchOperator spliter = new SplitBatchOp().setFraction(0.6).setRandomSeed(1);
		BatchOperator left = spliter.linkFrom(data);
		BatchOperator right = spliter.getSideOutput(0);
		Assert.assertEquals(left.count(), 90);
		Assert.assertEquals(right.count(), 60);
		Assert.assertEquals(new UnionBatchOp().linkFrom(left, right).count(), 147);
	}

	@Test
	public void splitWithSameSample() throws Exception {
		BatchOperator data = Iris.getBatchData();
		BatchOperator source = new DataSetWrapperBatchOp(
			data.getDataSet().union(data.getDataSet()).union(data.getDataSet()),
			data.getColNames(), data.getColTypes());
		BatchOperator spliter = new SplitBatchOp().setFraction(0.01).setRandomSeed(1);
		BatchOperator left = spliter.linkFrom(source);
		BatchOperator right = spliter.getSideOutput(0);
		List <Row> list = left.collect();
		Assert.assertEquals(list.size(), 5);
		Assert.assertEquals((double) list.get(0).getField(0), 6.1, 0.01);
		Assert.assertEquals((double) list.get(1).getField(0), 6.8, 0.01);
		Assert.assertEquals((double) list.get(2).getField(0), 7.1, 0.01);
		Assert.assertEquals((double) list.get(3).getField(0), 6.4, 0.01);
		Assert.assertEquals((double) list.get(4).getField(0), 4.4, 0.01);
		Assert.assertEquals(right.count(), 445);
	}
}