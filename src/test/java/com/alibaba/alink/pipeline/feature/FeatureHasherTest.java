package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for FeatureHasher.
 */
public class FeatureHasherTest {
	@Test
	public void test() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.1, true, "2", "A"}),
			Row.of(new Object[] {1.1, false, "2", "B"}),
			Row.of(new Object[] {1.1, true, "1", "B"}),
			Row.of(new Object[] {2.2, true, "1", "A"}),
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"double", "bool", "number", "str"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"double", "bool", "number", "str"});

		FeatureHasher op = new FeatureHasher()
			.setSelectedCols(new String[] {"double", "bool", "number", "str"})
			.setNumFeatures(100)
			.setOutputCol("features");

		Table res = op.transform(data);

		List <Vector> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("features"), Vector.class)
			.collect();

		Assert.assertArrayEquals(list.toArray(new SparseVector[list.size()]), new SparseVector[] {
			new SparseVector(100, new int[]{9, 38, 45, 95}, new double[]{1.0, 1.1, 1.0, 1.0}),
			new SparseVector(100, new int[]{9, 30, 38, 76}, new double[]{1.0, 1.0, 1.1, 1.0}),
			new SparseVector(100, new int[]{11, 38, 76, 95}, new double[]{1.0, 1.1, 1.0, 1.0}),
			new SparseVector(100, new int[]{11, 38, 45, 95}, new double[]{1.0, 2.2, 1.0, 1.0})});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}
}
