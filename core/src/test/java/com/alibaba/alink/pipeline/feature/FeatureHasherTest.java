package com.alibaba.alink.pipeline.feature;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.FeatureHasherBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.FeatureHasherStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for FeatureHasher.
 */
public class FeatureHasherTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.1, true, "2", "A"}),
			Row.of(new Object[] {1.1, false, "2", "B"}),
			Row.of(new Object[] {1.1, true, "1", "B"}),
			Row.of(new Object[] {2.2, true, "1", "A"}),
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows,
			new String[] {"double", "bool", "number", "str"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows,
			new String[] {"double", "bool", "number", "str"});

		FeatureHasher op = new FeatureHasher()
			.setSelectedCols(new String[] {"double", "bool", "number", "str"})
			.setNumFeatures(100)
			.setOutputCol("features");

		Table res = op.transform(data);

		Assert.assertArrayEquals(
			MLEnvironmentFactory.getDefault()
				.getBatchTableEnvironment()
				.toDataSet(res.select("features"), new RowTypeInfo(VectorTypes.VECTOR))
				.collect()
				.stream()
				.map(row -> (SparseVector) row.getField(0))
				.toArray(SparseVector[]::new),
			new SparseVector[] {
				new SparseVector(100, new int[] {9, 38, 45, 95}, new double[] {1.0, 1.1, 1.0, 1.0}),
				new SparseVector(100, new int[] {9, 30, 38, 76}, new double[] {1.0, 1.0, 1.1, 1.0}),
				new SparseVector(100, new int[] {11, 38, 76, 95}, new double[] {1.0, 1.1, 1.0, 1.0}),
				new SparseVector(100, new int[] {11, 38, 45, 95}, new double[] {1.0, 2.2, 1.0, 1.0})}
		);

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testInitializer() {
		FeatureHasher op = new FeatureHasher(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new FeatureHasherBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new FeatureHasherBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new FeatureHasherStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new FeatureHasherStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
